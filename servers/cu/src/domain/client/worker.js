import { workerData } from 'node:worker_threads'
import { Writable, Readable, pipeline } from 'node:stream'
import { createReadStream, createWriteStream } from 'node:fs'
import { join } from 'node:path'
import { createGunzip, createGzip } from 'node:zlib'
import { promisify } from 'node:util'
import { hostname } from 'node:os'
import { randomBytes } from 'node:crypto'

import { worker } from 'workerpool'
import { T, always, applySpec, assocPath, cond, defaultTo, identity, ifElse, is, pathOr, pipe, propOr } from 'ramda'
import { LRUCache } from 'lru-cache'
import { Rejected, Resolved, fromPromise, of } from 'hyper-async'
import AoLoader from '@permaweb/ao-loader'

import { createLogger } from '../logger.js'

const pipelineP = promisify(pipeline)

function wasmResponse (stream) {
  return new Response(stream, { headers: { 'Content-Type': 'application/wasm' } })
}

/**
 * ###################
 * File Utils
 * ###################
 */

function readWasmFileWith ({ DIR }) {
  return async (moduleId) => {
    const file = join(DIR, `${moduleId}.wasm.gz`)

    return new Promise((resolve, reject) =>
      resolve(pipeline(
        createReadStream(file),
        createGunzip(),
        reject
      ))
    )
  }
}

function writeWasmFileWith ({ DIR, logger }) {
  return async (moduleId, wasmStream) => {
    const file = join(DIR, `${moduleId}.wasm.gz`)

    return pipelineP(
      wasmStream,
      createGzip(),
      createWriteStream(file)
    ).catch((err) => {
      logger('Failed to cache binary for module "%s" in a file. Skipping...', moduleId, err)
    })
  }
}

/**
 * #######################
 * Network Utils
 * #######################
 */

function streamTransactionDataWith ({ fetch, GATEWAY_URL, logger }) {
  return (id) =>
    of(id)
      .chain(fromPromise((id) =>
        fetch(`${GATEWAY_URL}/raw/${id}`)
          .then(async (res) => {
            if (res.ok) return res
            logger(
              'Error Encountered when fetching raw data for transaction \'%s\' from gateway \'%s\'',
              id,
              GATEWAY_URL
            )
            throw new Error(`${res.status}: ${await res.text()}`)
          })
      ))
      .toPromise()
}

/**
 * ##############################
 * #### LRU In-Memory Cache utils
 * ##############################
 */

/**
 * A cache for compiled Wasm Modules
 *
 * @returns {LRUCache<string, WebAssembly.Module>}
 */
function createWasmModuleCache ({ MAX_SIZE }) {
  return new LRUCache({
    /**
     * #######################
     * Capacity Configuration
     * #######################
     */
    max: MAX_SIZE
  })
}

/**
 * A cache for loaded wasm modules,
 * as part of evaluating a stream of messages
 *
 * @returns {LRUCache<string, Function>}
 */
function createWasmInstanceCache ({ MAX_SIZE }) {
  return new LRUCache({
    /**
     * #######################
     * Capacity Configuration
     * #######################
     */
    max: MAX_SIZE
  })
}

export function evaluateWith ({
  wasmInstanceCache,
  wasmModuleCache,
  readWasmFile,
  writeWasmFile,
  streamTransactionData,
  bootstrapWasmInstance,
  logger
}) {
  function maybeCachedModule ({ streamId, moduleId, gas, memLimit, Memory, message, AoGlobal }) {
    return of(moduleId)
      .map((moduleId) => wasmModuleCache.get(moduleId))
      .chain((wasm) => wasm
        ? Resolved(wasm)
        : Rejected({ streamId, moduleId, gas, memLimit, Memory, message, AoGlobal })
      )
  }

  function maybeStoredBinary ({ streamId, moduleId, gas, memLimit, Memory, message, AoGlobal }) {
    logger('Checking for wasm file to load module "%s"...', moduleId)

    return of(moduleId)
      .chain(fromPromise(readWasmFile))
      .chain(fromPromise((stream) =>
        WebAssembly.compileStreaming(wasmResponse(Readable.toWeb(stream)))
      ))
      .bimap(
        () => ({ streamId, moduleId, gas, memLimit, Memory, message, AoGlobal }),
        identity
      )
  }

  function loadTransaction ({ moduleId }) {
    logger('Loading wasm transaction "%s"...', moduleId)

    return of(moduleId)
      .chain(fromPromise(streamTransactionData))
      .map((res) => res.body.tee())
      /**
       * Simoultaneously cache the binary in a file
       * and compile to a WebAssembly.Module
       */
      .chain(fromPromise(([s1, s2]) =>
        Promise.all([
          writeWasmFile(moduleId, Readable.fromWeb(s1)),
          WebAssembly.compileStreaming(wasmResponse(s2))
        ])
      ))
      .map(([, res]) => res)
  }

  function loadInstance ({ streamId, moduleId, gas, memLimit, Memory, message, AoGlobal }) {
    return maybeCachedModule({ streamId, moduleId, gas, memLimit, Memory, message, AoGlobal })
      .bichain(
        /**
         * Potentially Compile the Wasm Module, cache it for next time,
         *
         * then create the Wasm instance
         */
        () => of({ streamId, moduleId, gas, memLimit, Memory, message, AoGlobal })
          .chain(maybeStoredBinary)
          .bichain(loadTransaction, Resolved)
          /**
           * Cache the wasm Module in memory for quick access next time
           */
          .map((wasmModule) => {
            logger('Caching compiled WebAssembly.Module for module "%s" in memory, for next time...', moduleId)
            wasmModuleCache.set(moduleId, wasmModule)
            return wasmModule
          }),
        /**
         * Cached instance, so just reuse
         */
        Resolved
      )
      .chain(fromPromise((wasmModule) => bootstrapWasmInstance(wasmModule)))
      /**
       * Cache the wasm module for this particular stream,
       * in memory, for quick retrieval next time
       */
      .map((wasmInstance) => {
        wasmInstanceCache.set(streamId, wasmInstance)
        return wasmInstance
      })
  }

  function maybeCachedInstance ({ streamId, moduleId, gas, memLimit, Memory, message, AoGlobal }) {
    return of(streamId)
      .map((streamId) => wasmInstanceCache.get(streamId))
      .chain((wasmInstance) => wasmInstance
        ? Resolved(wasmInstance)
        : Rejected({ streamId, moduleId, gas, memLimit, Memory, message, AoGlobal })
      )
  }

  /**
   * Given the previous interaction output,
   * return a function that will merge the next interaction output
   * with the previous.
   */
  const mergeOutput = (prevMemory) => pipe(
    defaultTo({}),
    applySpec({
      /**
       * If the output contains an error, ignore its state,
       * and use the previous evaluation's state
       */
      Memory: ifElse(
        pathOr(undefined, ['Error']),
        always(prevMemory),
        propOr(prevMemory, 'Memory')
      ),
      Error: pathOr(undefined, ['Error']),
      Messages: pathOr([], ['Messages']),
      Spawns: pathOr([], ['Spawns']),
      Output: pipe(
        pathOr('', ['Output']),
        /**
         * Always make sure Output
         * is a string or object
         */
        cond([
          [is(String), identity],
          [is(Object), identity],
          [is(Number), String],
          [T, identity]
        ])
      ),
      GasUsed: pathOr(undefined, ['GasUsed'])
    })
  )

  /**
   * Evaluate a message using the handler that wraps the WebAssembly.Instance,
   * identified by the streamId.
   *
   * If not already instantiated and cached in memory, attempt to use a cached WebAssembly.Module
   * and instantiate the Instance and handler, caching it by streamId
   *
   * If the WebAssembly.Module is not cached, then we check if the binary is cached in a file,
   * then compile it in a WebAssembly.Module, cached in memory, then used to instantiate a
   * new WebAssembly.Instance
   *
   * If not in a file, then the module transaction is downloaded from the Gateway url,
   * cached in a file, compiled, further cached in memory, then used to instantiate a
   * new WebAssembly.Instance and handler
   *
   * Finally, evaluates the message and returns the result of the evaluation.
   */
  return ({ streamId, moduleId, gas, memLimit, name, processId, Memory, message, AoGlobal }) =>
    /**
     * Dynamically load the module, either from cache,
     * or from a file
     */
    maybeCachedInstance({ streamId, moduleId, gas, memLimit, name, processId, Memory, message, AoGlobal })
      .bichain(loadInstance, Resolved)
      /**
       * Perform the evaluation
       */
      .chain((wasmInstance) =>
        of(wasmInstance)
          .map((wasmInstance) => {
            logger('Evaluating message "%s" to process "%s"', name, processId)
            return wasmInstance
          })
          .chain(fromPromise(async (wasmInstance) => wasmInstance(Memory, message, AoGlobal)))
          .bichain(
            /**
             * Map thrown error to a result.error. In this way, the Worker should _never_
             * throw due to evaluation
             *
             * TODO: should we also evict the wasmInstance from cache, so it's reinstantaited
             * with the new memory for next time?
             */
            (err) => Resolved(assocPath(['Error'], err, {})),
            Resolved
          )
          .map(mergeOutput(Memory))
      )
      .toPromise()
}

/**
 * ########################
 * ### Cron Generation ####
 * ########################
 */

const toSeconds = (millis) => Math.floor(millis / 1000)

/**
 * Whether the block height, relative to the origin block height,
 * matches the provided cron
 */
function isBlockOnCron ({ height, originHeight, cron }) {
  /**
   * Don't count the origin height as a match
   */
  if (height === originHeight) return false

  return (height - originHeight) % cron.value === 0
}

/**
 * Whether the timstamp, relative to the origin timestamp,
 * matches the provided cron
 */
function isTimestampOnCron ({ timestamp, originTimestamp, cron }) {
  /**
   * The smallest unit of time a cron can be placed is in seconds,
   * and if we modulo milliseconds, it can return 0 for fractional overlaps
   * of the scedule
   *
   * So convert the times to seconds perform applying modulo
   */
  timestamp = toSeconds(timestamp)
  originTimestamp = toSeconds(originTimestamp)
  /**
   * don't count the origin timestamp as a match
   */
  if (timestamp === originTimestamp) return false
  return (timestamp - originTimestamp) % cron.value === 0
}

class CronMessagesStream extends Writable {
  constructor (options, { streamId, maxBuffer, fileDirectory }) {
    super({ ...options, objectMode: true })

    this.streamId = streamId
    this.fileDirectory = fileDirectory
    /**
     * Default to 100 cron messages to be buffered in memory
     * before switching to a file
     */
    this.maxBuffer = maxBuffer || 100
    this.cronMessages = []
  }

  _write (message, _encoding, cb) {
    /**
     * Haven't needed to swap to buffering
     * into a file yet
     */
    if (!this.fileStream) {
      this.cronMessages.push(message)
      /**
       * Our max buffer size has been reached. Swap to using a file to prevent
       * memory pressure from getting too high
       */
      if (this.cronMessages.length > this.maxBuffer) {
        this.filePath = join(this.fileDirectory, `${this.streamId}-${randomBytes(8).toString('hex')}`)
        this.fileStream = createWriteStream(this.filePath)
        let m
        /**
         * write each message as a single line
         */
        while ((m = this.cronMessages.shift()) !== null) this.fileStream.write(`${JSON.stringify(m)}\r\n`, cb)
      } else cb()

    /**
     * Using a file so just write the message as a single line
     */
    } else this.fileStream.write(`${JSON.stringify(message)}\r\n`, cb)
  }

  /**
   * Either return the array of cron messages
   * or the path to the file containing the cron messages (in the case that
   * there were too many cron messages generated to fit in the buffer)
   */
  fork () {
    if (this.closed) throw new Error('stream has already been closed')

    this.end()
    return this.filePath || this.cronMessages
  }
}

function cronMessagesBetweenWith ({ DIR }) {
  return ({ streamId, processId, processOwner, originBlock, blockBased, timeBased, blocksMeta, left, right }) => {
    const cronMessagesStream = new CronMessagesStream(undefined, { streamId, fileDirectory: DIR })

    /**
     * { height, timestamp }
     */
    const leftBlock = left.block
    const rightBlock = right.block
    const leftOrdinate = left.ordinate

    /**
       * Grab the blocks that are between the left and right boundary,
       * according to their timestamp
       */
    const blocksInRange = blocksMeta.filter((b) =>
      b.timestamp > leftBlock.timestamp &&
        b.timestamp < rightBlock.timestamp
    )

    /**
       * Start at the left block timestamp, incrementing one second per iteration.
       * - if our current time gets up to the next block, then check for any block-based cron messages to generate
       * - Check for any timebased crons to generate on each tick
       *
       * The curBlock always starts at the leftBlock, then increments as we tick
       */
    let curBlock = leftBlock
    for (let curTimestamp = leftBlock.timestamp; curTimestamp < rightBlock.timestamp; curTimestamp += 1000) {
      /**
         * We've ticked up to our next block
         * so check if it's on a Cron Interval
         *
         * This way, Block-based messages will always be pushed onto the stream of messages
         * before time-based messages
         */
      const nextBlock = blocksInRange[0]
      if (nextBlock && toSeconds(curTimestamp) >= toSeconds(nextBlock.timestamp)) {
        /**
           * Make sure to remove the block from our range,
           * since we've ticked past it,
           *
           * and save it as the new current block
           */
        curBlock = blocksInRange.shift()

        for (let i = 0; i < blockBased.length; i++) {
          const cron = blockBased[i]

          if (isBlockOnCron({ height: curBlock.height, originHeight: originBlock.height, cron })) {
            cronMessagesStream.write({
              cron: `${i}-${cron.interval}`,
              ordinate: leftOrdinate,
              name: `Cron Message ${curBlock.timestamp},${leftOrdinate},${i}-${cron.interval}`,
              message: {
                Owner: processOwner,
                Target: processId,
                From: processOwner,
                Tags: cron.message.tags,
                Timestamp: curBlock.timestamp,
                'Block-Height': curBlock.height,
                Cron: true
              }
            })
          }
        }
      }

      for (let i = 0; i < timeBased.length; i++) {
        const cron = timeBased[i]

        if (isTimestampOnCron({ timestamp: curTimestamp, originTimestamp: originBlock.timestamp, cron })) {
          cronMessagesStream.write({
            cron: `${i}-${cron.interval}`,
            ordinate: leftOrdinate,
            name: `Cron Message ${curTimestamp},${leftOrdinate},${i}-${cron.interval}`,
            message: {
              Owner: processOwner,
              Target: processId,
              From: processOwner,
              Tags: cron.message.tags,
              Timestamp: curTimestamp,
              'Block-Height': curBlock.height,
              Cron: true
            }
          })
        }
      }
    }

    return cronMessagesStream.fork()
  }
}

/**
 * ##################
 * ### Worker API ###
 * ##################
 */

if (!process.env.NO_WORKER) {
  const logger = createLogger(`ao-cu:${hostname()}:worker-${workerData.id}`)
  /**
   * Expose our worker api
   */
  worker({
    evaluate: evaluateWith({
      wasmModuleCache: createWasmModuleCache({ MAX_SIZE: workerData.WASM_MODULE_CACHE_MAX_SIZE }),
      wasmInstanceCache: createWasmInstanceCache({ MAX_SIZE: workerData.WASM_INSTANCE_CACHE_MAX_SIZE }),
      readWasmFile: readWasmFileWith({ DIR: workerData.WASM_BINARY_FILE_DIRECTORY }),
      writeWasmFile: writeWasmFileWith({ DIR: workerData.WASM_BINARY_FILE_DIRECTORY, logger }),
      streamTransactionData: streamTransactionDataWith({ fetch, GATEWAY_URL: workerData.GATEWAY_URL, logger }),
      bootstrapWasmInstance: (wasmModule) => AoLoader((info, receiveInstance) =>
        WebAssembly.instantiate(wasmModule, info).then(receiveInstance)
      ),
      logger
    }),
    cronMessagesBetween: cronMessagesBetweenWith({ DIR: workerData.CRON_MESSAGES_FILE_DIRECTORY })
  })
}
