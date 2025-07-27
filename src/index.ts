import { AsyncLocalStorage } from "async_hooks"
import { randomUUID } from "crypto"

// a symbol for per-instance pending-ops queue
const PENDING = Symbol("pendingOps")

// AsyncLocalStorage for transaction context
const transactionContext = new AsyncLocalStorage<any>()

const dummyBackend: Map<string, any> = new Map()

// Fake stubs for remote operations
async function fetchRemote<T>(key: string): Promise<T | undefined> {
  const result = dummyBackend.get(key)
  const currentTxn = getCurrentTransaction()
  if (currentTxn) {
    console.log(
      `[fetchRemote] ${key} = ${result} (within transaction ${currentTxn.id})`
    )
  } else {
    console.log(`[fetchRemote] ${key} = ${result} (no transaction context)`)
  }
  return result
}
async function writeRemote<T>(txn: any, key: string, v: T): Promise<void> {
  const currentTxn = getCurrentTransaction()
  if (currentTxn) {
    console.log(
      `[writeRemote] ${key} = ${v} (within transaction ${currentTxn.id})`
    )
  } else {
    console.log(`[writeRemote] ${key} = ${v} (no transaction context)`)
  }
  dummyBackend.set(key, v)
}

// property decorator
function AsyncSync<T, V>(
  target: undefined,
  context: ClassFieldDecoratorContext<T, V>
) {
  const prop = context.name as string
  const backing = Symbol(prop)
  console.log(`[AsyncSync] decorator applied to property: ${prop}`)

  context.addInitializer(function (this: any) {
    console.log(`[AsyncSync] initializer called for property: ${prop}`)

    // Define the property with getter/setter
    Object.defineProperty(this, prop, {
      get(): Promise<any> {
        // Return a promise that resolves with the value
        return (async () => {
          if (this[backing] !== undefined) {
            console.log(`[get] ${prop} (cached) = ${this[backing]}`)
            return this[backing]
          }
          console.log(`[get] ${prop} (fetching...)`)
          const val = await fetchRemote(prop)
          this[backing] = val
          return val
        })()
      },
      set(v: any) {
        console.log(`[set] ${prop} = ${v}`)
        // queue the write
        const transactionalWrite = (txn: any) => {
          return writeRemote(txn, prop, v)
        }
        ;(this[PENDING] ||= []).push(transactionalWrite)
        // Push the value locally so we can read our writes
        this[backing] = v
      },
      enumerable: true,
      configurable: true,
    })
  })

  // Return a function that doesn't set an initial value
  return function () {
    return undefined as any
  }
}

// Method decorator for transactional context
function Transactional<T, Args extends any[], Return>(
  target: (this: T, ...args: Args) => Return,
  context: ClassMethodDecoratorContext<T, (this: T, ...args: Args) => Return>
) {
  const methodName = String(context.name)
  console.log(`[Transactional] decorator applied to method: ${methodName}`)

  return function (this: T, ...args: Args): Return {
    console.log(`[Transactional] ${methodName} - starting transaction context`)

    // Create a transaction object for this execution
    const transaction = {
      id: randomUUID(),
      startTime: Date.now(),
      method: methodName,
    }

    // Execute the original method within the AsyncLocalContext
    return transactionContext.run(transaction, () => {
      console.log(
        `[Transactional] ${methodName} - executing within transaction ${transaction.id}`
      )
      try {
        const result = target.call(this, ...args)

        // Handle both sync and async methods
        if (result instanceof Promise) {
          return result.then(
            async (value) => {
              console.log(
                `[Transactional] ${methodName} - transaction ${transaction.id} completed successfully`
              )
              // Auto-flush on successful completion
              if (typeof (this as any).flush === 'function') {
                console.log(`[Transactional] ${methodName} - auto-flushing transaction ${transaction.id}`)
                await (this as any).flush()
              }
              return value
            },
            (error) => {
              console.log(
                `[Transactional] ${methodName} - transaction ${transaction.id} failed with error:`,
                error.message
              )
              throw error
            }
          ) as Return
        } else {
          console.log(
            `[Transactional] ${methodName} - transaction ${transaction.id} completed successfully`
          )
          // Auto-flush on successful completion (sync case)
          if (typeof (this as any).flush === 'function') {
            console.log(`[Transactional] ${methodName} - auto-flushing transaction ${transaction.id}`)
            // For sync methods, we need to return a promise if flush is async
            const flushResult = (this as any).flush()
            if (flushResult instanceof Promise) {
              return flushResult.then(() => result) as Return
            }
          }
          return result
        }
      } catch (error) {
        console.log(
          `[Transactional] ${methodName} - transaction ${transaction.id} failed with error:`,
          (error as Error).message
        )
        throw error
      }
    })
  }
}

// Helper function to get current transaction context
function getCurrentTransaction() {
  return transactionContext.getStore()
}

// TODO: maybe move this into the Transactional decorator?
class FlushableCls {
  async flush() {
    const q: any[] = (this as any)[PENDING] || []
    console.log(`[flush] executing ${q.length} pending operations`)
    const fakeTransaction = {} // dummy transaction object
    await Promise.all(q.map((op) => op(fakeTransaction)))
    // Clear the queue after flushing
    ;(this as any)[PENDING] = []
  }
}

// @WithFlush
// class MyThing {
class MyThing extends FlushableCls {
  @AsyncSync
  name!: string

  @AsyncSync
  age!: number

  @Transactional
  async doWork() {
    const txn = getCurrentTransaction()
    console.log("[doWork] current name", await (this as any).name + "in txn " + txn?.id)
    ;(this as any).name = "John"
    console.log("[doWork] new name", await (this as any).name + "in txn " + txn?.id)
    // Returns successfully, the property will be updated in the remote backend
  }

  @Transactional
  async readValue() {
    const txn = getCurrentTransaction()
    console.log("[readValue] current name", await (this as any).name + "in txn " + txn?.id)
  }

  @Transactional
  async moreWork() {
    const txn = getCurrentTransaction()
    console.log("[moreWork] current name", await (this as any).name + "in txn " + txn?.id)
    ;(this as any).name = "Jane"
    console.log("[moreWork] new name", await (this as any).name + "in txn " + txn?.id)
    throw new Error("oops")
    // Returns unsuccessfully, the property will not be updated in the remote backend
  }
}

// 1. Create a new instance of MyThing
let thing = new MyThing()

// 2. Call doWork
await thing.doWork()
await thing.readValue()

// 3. Create a new instance of MyThing, see value still there
thing = new MyThing()
await thing.readValue()

// 4. Call moreWork, see value not updated (transaction fails, no flush)
try {
  thing = new MyThing()
  await thing.moreWork()
} catch (error) {
  console.log("(expected) error", error)
}

// 5. Call readValue to see value not updated (since moreWork failed)
thing = new MyThing()
await thing.readValue()
