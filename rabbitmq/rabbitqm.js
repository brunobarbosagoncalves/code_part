import amqp from "amqplib"

export class Amqp {
  _config = {}

  constructor(config = { connection: {}, queue: {} }) {
    this._config = {
      connection: {
        protocol: process.env.RABBITMQ_PROTOCOL || "amqp",
        hostname: process.env.RABBITMQ_HOSTNAME || "localhost",
        username: process.env.RABBITMQ_USERNAME || "guest",
        password: process.env.RABBITMQ_PASSWORD || "guest",
        port: process.env.RABBITMQ_PORT || 5672,
        ...(config.connection || {}),
      },
      queue: {
        durable: false,
        arguments: {
          "x-message-ttl": 5000, //After 5000 miliseconds the message are deleted automatic
          "x-expires": 10000, //After 10000 miliseconds the queue are delete if not have activity(consumers working)
        },
        ...(config.queue || {}),
      },
    }

    this.init()
  }

  init = async () => {}

  connect = async () => {
    try {
      return await amqp.connect({ ...this._config.connection })
    } catch (error) {
      return Promise.reject({
        status: false,
        message: "[Amqp] Error connect rabbit",
        error,
      })
    }
  }

  channel = async (connection) => {
    try {
      return await connection.createChannel()
    } catch (error) {
      return Promise.reject({
        status: false,
        message: "[Amqp] Error create channel",
        error,
      })
    }
  }

  toBuffer = async (payload) => {
    try {
      if ([null, undefined].includes(payload)) {
        return Buffer.from("")
      }

      if (typeof payload === "object") {
        return Buffer.from(JSON.stringify(payload))
      }

      return Buffer.from(String(payload))
    } catch (error) {
      return Promise.reject({
        status: false,
        message: "[Amqp] Error toBuffer ",
        error,
      })
    }
  }

  /**
   *
   * @param {*} param0
   */
  publisher = async ({ channelName, payload }) => {
    try {
      const connection = await this.connect()

      const channel = await this.channel(connection)

      await channel.assertQueue(channelName, { ...this._config.queue })

      await channel.sendToQueue(channelName, await this.toBuffer(payload))

      console.log(`Sending message: `, payload)

      //await publish to close connection
      new Promise((res, rej) => {
        setTimeout(() => {
          return res(connection.close())
        }, 500)
      })
    } catch (error) {
      console.log(error)
      return Promise.reject({
        status: false,
        message: "[Amqp] Error publisher",
        error,
      })
    }
  }

  consumer = async ({
    channelName,
    callback,
    consumeConfig = { keepSocket: false },
  }) => {
    try {
      const connection = await this.connect()

      const channel = await this.channel(connection)

      channel.assertQueue(channelName, { ...this._config.queue })

      channel.consume(channelName, (message) => {
        const payload = JSON.parse(message.content.toString())
        console.log(`Received:`)
        console.log(payload)

        if (typeof callback === "function") {
          try {
            callback(payload)
          } catch (error) {
            return Promise.reject({
              status: false,
              message: "[Amqp] Error listen callback",
              error,
            })
          }
        }

        // remove message from queue after loaded from consumer
        channel.ack(message)
      })

      // close socket after execute consume, and prevent socket listening
      if (!consumeConfig.keepSocket || consumeConfig.keepSocket === false) {
        new Promise((res, rej) => {
          setTimeout(() => {
            return res(connection.close())
          }, 500)
        })
      }
      console.log(`Waiting for messages...`)
    } catch (error) {
      console.log(error)
      return Promise.reject({
        status: false,
        message: "[Amqp] Error consumer",
        error,
      })
    }
  }
}
