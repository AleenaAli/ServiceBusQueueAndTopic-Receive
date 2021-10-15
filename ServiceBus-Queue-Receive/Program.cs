using Azure.Messaging.ServiceBus;
using System;
using System.Threading.Tasks;

namespace ServiceBusQueueAndTopic_Receive
{
    class Program
    {
        private static string connectionStringForQueue = "Endpoint=sb://sb-ordersdemo.servicebus.windows.net/;SharedAccessKeyName=Listen;SharedAccessKey=FoLprUX3kQbApke05sFtR6asM7drbPtWV/97RvCfpoU=;EntityPath=ordersqueue";
        private static string queueName = "ordersqueue";

        //Policies for Topics
        private static string connectionStringForTopic = "Endpoint=sb://sb-ordersdemo.servicebus.windows.net/;SharedAccessKeyName=Listen;SharedAccessKey=KHYLZniNTfVOAjc/h7qDg2j5pzLjg3+sJnjjxNeTAO8=;EntityPath=orderstopic";
        private static string topicName = "orderstopic";
        private static string subscriptionName = "subscriptionA";



        public static async Task Main(string[] args)
        {
            //await ListenQueueMessage();
            //await ReceiveAndDeleteQueueMessage();

            await ReceiveAndDeleteSubscriptionMessage();
        }

        public static async Task ListenQueueMessage()
        {

            ServiceBusClient client = new ServiceBusClient(connectionStringForQueue);
            ServiceBusReceiver receiver = client.CreateReceiver(queueName, new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.PeekLock });

            ServiceBusReceivedMessage message = await receiver.ReceiveMessageAsync();

            Console.WriteLine("From the function ListenQueueMessage, peeking into Queue Storage.");
            Console.WriteLine(message.Body);
            Console.WriteLine($"The Sequence number is {message.SequenceNumber}");
            Console.WriteLine(message);
        }

        public static async Task ReceiveAndDeleteQueueMessage()
        {

            ServiceBusClient client = new ServiceBusClient(connectionStringForQueue);
            ServiceBusReceiver receiver = client.CreateReceiver(queueName, new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });

            var messages = await receiver.ReceiveMessagesAsync(5);

            Console.WriteLine("From the function ReceiveAndDeleteQueueMessage, receiving messages from Queue Storage.");
            foreach (var message in messages)
            {
                Console.WriteLine(message.Body);
                Console.WriteLine($"The sequence number is {message.SequenceNumber}");
            }

        }

        public static async Task ReceiveAndDeleteSubscriptionMessage()
        {

            ServiceBusClient client = new ServiceBusClient(connectionStringForTopic);
            ServiceBusReceiver receiver = client.CreateReceiver(topicName, subscriptionName, new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });

            var messages = await receiver.ReceiveMessagesAsync(5);

            Console.WriteLine("From the function ReceiveAndDeleteSubscriptionMessage, receiving messages from Topic.");
            foreach (var message in messages)
            {
                Console.WriteLine(message.Body);
                Console.WriteLine($"The sequence number is {message.SequenceNumber}");
            }

        }
    }
}
