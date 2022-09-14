import { Amqp } from './rabbitqm.js'

const amqp = new Amqp({})

//consumer
const callback = data => console.log(`[Amqp] Consumer <= ${data}`) 

const channelName = 'myChannelName'

const errorHandler = error => console.log({
    message: "[Amqp] Error Consumer",
    error,
})

/**
 * @augments channelName String: required
 * @argument callback Function: required
 * @argument consumeConfig Object: not required
 * @argument consumeConfig.keepSocket Boolean: not required: 
 *      true: The socket keep listening forever
 *      false: After request done socket close
 */
await amqp
    .consumer({
        channelName,
        callback,
        consumeConfig: { keepSocket: false },// true | false
    })
    .catch((error) => {
        errorHandler({
            message: "[Amqp] Error Consumer",
            error,
        })
    })

//publisher
const payload = {
    target: `Posted to ${target}`,
    date: new Date().toISOString(),
    //other fields
}

/**
 * @augments channelName String: required
 * @argument payload Object: not required
 */
await amqp
    .publisher({ 
        channelName,
        payload,
    })
    .catch((error) => {
        errorHandler({
            message: "[Amqp] Error Publisher",
            error,
        })
})
