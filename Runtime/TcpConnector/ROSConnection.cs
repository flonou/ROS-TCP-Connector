using RosMessageGeneration;
using RosMessageTypes.TcpEndpoint;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using UnityEngine;

public class ROSConnection : MonoBehaviour
{
    // Variables required for ROS communication
    public string hostName = "192.168.1.1";
    public int hostPort = 10000;
    [Tooltip("If blank, determine IP automatically.")]
    public string overrideUnityIP = "";
    public int unityPort = 5005;
    bool alreadyStartedServer = false;

    private int networkTimeout = 2000;

    public int awaitDataMaxRetries = 10;
    public float awaitDataSleepSeconds = 1.0f;

    TcpClient persistantClient;
    NetworkStream persistantNetworkStream;

    public bool keepConnection = true;
    protected bool connecting = false;

    protected bool running;

    static object _lock = new object(); // sync lock 
    static object _connectionLock = new object(); // sync lock 
    static List<Task> activeConnectionTasks = new List<Task>(); // pending connections

    const string ERROR_TOPIC_NAME = "__error";
    const string HANDSHAKE_TOPIC_NAME = "__handshake";
    const string CONNECTION_PARAM_TOPIC_NAME = "__connection_param";

    struct SubscriberCallback
    {
        public ConstructorInfo messageConstructor;
        public List<Action<Message>> callbacks;
    }

    Dictionary<string, SubscriberCallback> subscribers = new Dictionary<string, SubscriberCallback>();

    void OnDisable()
    {
        running = false;
    }

    void OnEnable()
    {
		running = true;
        Subscribe<RosUnityError>(ERROR_TOPIC_NAME, RosUnityErrorCallback);
        if (overrideUnityIP != "")
        {
            new Thread(() => StartMessageServer(overrideUnityIP, unityPort)).Start(); // no reason to wait, if we already know the IP
            //StartMessageServer(overrideUnityIP, unityPort); // no reason to wait, if we already know the IP
        }

        Send(CONNECTION_PARAM_TOPIC_NAME, new RosUnityConnectionParam(keepConnection));
        Debug.Log("sent connection parameters to the server");

        SendServiceMessage<RosUnityHandshakeResponse>(HANDSHAKE_TOPIC_NAME, new RosUnityHandshakeRequest(overrideUnityIP, (ushort)unityPort), RosUnityHandshakeCallback);
    }

    void RosUnityHandshakeCallback(RosUnityHandshakeResponse response)
    {
        new Thread(() => StartMessageServer(response.ip, unityPort)).Start();
        //StartMessageServer(response.ip, unityPort);
    }

    void RosUnityErrorCallback(RosUnityError error)
    {
        Debug.LogError("ROS-Unity error: " + error.message);
    }

    public void Subscribe<T>(string topic, Action<T> callback) where T : Message, new()
    {
        SubscriberCallback subCallbacks;
        if (!subscribers.TryGetValue(topic, out subCallbacks))
        {
            subCallbacks = new SubscriberCallback
            {
                messageConstructor = typeof(T).GetConstructor(new Type[0]),
                callbacks = new List<Action<Message>> { }
            };
            subscribers.Add(topic, subCallbacks);
        }

        subCallbacks.callbacks.Add((Message msg) => { callback((T)msg); });
    }

    /// <summary>
    /// 	Function is meant to be overridden by inheriting classes to specify how to handle read messages.
    /// </summary>
    /// <param name="tcpClient"></param> TcpClient to read byte stream from.
    protected async Task HandleConnectionAsync(TcpClient tcpClient)
    {
        //await Task.Yield();
        NetworkStream networkStream = tcpClient.GetStream();
        // continue asynchronously on another thread
        do
        {
            //Debug.Log("start reading at : " + System.DateTime.Now.Millisecond);
            ReadMessage(networkStream);   
            //Debug.Log("       stop reading at : " + System.DateTime.Now.Millisecond);
        } while (keepConnection && running && tcpClient.Connected);        
    }

    void ReadMessage(NetworkStream networkStream)
    {
        try
        {
            if (networkStream.CanRead)
            {
                int offset = 0;

                // Get first bytes to determine length of topic name
                byte[] rawTopicBytes = new byte[4];
                networkStream.Read(rawTopicBytes, 0, rawTopicBytes.Length);
                offset += 4;
                int topicLength = BitConverter.ToInt32(rawTopicBytes, 0);

                // Read and convert topic name
                byte[] topicNameBytes = new byte[topicLength];
                networkStream.Read(topicNameBytes, 0, topicNameBytes.Length);
                offset += topicNameBytes.Length;
                string topicName = Encoding.ASCII.GetString(topicNameBytes, 0, topicLength);
                // TODO: use topic name to confirm proper received location

                //Debug.Log("got a message on topic : " + topicName);

                byte[] full_message_size_bytes = new byte[4];
                networkStream.Read(full_message_size_bytes, 0, full_message_size_bytes.Length);
                offset += 4;
                int full_message_size = BitConverter.ToInt32(full_message_size_bytes, 0);

                byte[] readBuffer = new byte[full_message_size];
                int numberOfBytesRead = 0;

                while (networkStream.DataAvailable && numberOfBytesRead < full_message_size)
                {
                    int bytesRead = networkStream.Read(readBuffer, 0, readBuffer.Length);
                    offset += bytesRead;
                    numberOfBytesRead += bytesRead;
                }

                SubscriberCallback subs;
                if (subscribers.TryGetValue(topicName, out subs))
                {
                    Message msg = (Message)subs.messageConstructor.Invoke(new object[0]);
                    msg.Deserialize(readBuffer, 0);
                    foreach (Action<Message> callback in subs.callbacks)
                    {
                        callback(msg);
                    }
                }
            }
        }
        catch (Exception e)
        {
            Debug.LogError("Exception raised!! " + e);
        }
    }

    /// <summary>
    /// 	Handles multiple connections and locks.
    /// </summary>
    /// <param name="tcpClient"></param> TcpClient to read byte stream from.
    private async Task StartHandleConnectionAsync(TcpClient tcpClient)
    {
        var connectionTask = HandleConnectionAsync(tcpClient);

        lock (_lock)
            activeConnectionTasks.Add(connectionTask);

        try
        {
            await connectionTask;
            // we may be on another thread after "await"
        }
        catch (Exception ex)
        {
            Debug.LogError(ex.ToString());
        }
        finally
        {
            lock (_lock)
                activeConnectionTasks.Remove(connectionTask);
        }
    }

    protected async void StartMessageServer(string ip, int port)
    {
        if (alreadyStartedServer)
            return;

        alreadyStartedServer = true;

        Debug.LogFormat("starting server on {0}:{1}", ip, port);
        while (running)
        {
            TcpListener tcpListener;
            try
            {
                tcpListener = new TcpListener(IPAddress.Parse(ip), port);
                tcpListener.Start();
            }
            catch (Exception e)
            {
                Debug.LogError("Exception raised!! " + e);
                return;
            }

            Debug.Log("ROS-Unity server listening on " + ip + ":" + port);

            try
            {
                while (running)   //we wait for a connection
                {
                    var tcpClient = await tcpListener.AcceptTcpClientAsync();

                    var task = StartHandleConnectionAsync(tcpClient);
                    // if already faulted, re-throw any error on the calling context
                    if (task.IsFaulted)
                        await task;
                }
            }
            catch (Exception e)
            {
                Debug.LogError("Exception raised!! " + e);
            }
        }
        alreadyStartedServer = false;
    }


    /// <summary>
    ///    Given some input values, fill a byte array in the desired format to use with
    ///     https://github.com/Unity-Technologies/Robotics-Tutorials/tree/master/catkin_ws/src/tcp_endpoint
    ///
    /// 	All messages are expected to come in the format of:
    /// 		first four bytes: int32 of the length of following string value
    /// 		next N bytes determined from previous four bytes: ROS topic name as a string
    /// 		next four bytes: int32 of the length of the remaining bytes for the ROS Message
    /// 		last N bytes determined from previous four bytes: ROS Message variables
    /// </summary>
    /// <param name="offset"></param> Index of where to start writing output data
    /// <param name="serviceName"></param> The name of the ROS service or topic that the message data is meant for
    /// <param name="fullMessageSizeBytes"></param> The full size of the already serialized message in bytes
    /// <param name="messageToSend"></param> The serialized ROS message to send to ROS network
    /// <returns></returns>
    public int GetPrefixBytes(int offset, byte[] serviceName, byte[] fullMessageSizeBytes, byte[] messagBuffer)
    {
        // Service Name bytes
        System.Buffer.BlockCopy(serviceName, 0, messagBuffer, 0, serviceName.Length);
        offset += serviceName.Length;

        // Full Message size bytes
        System.Buffer.BlockCopy(fullMessageSizeBytes, 0, messagBuffer, offset, fullMessageSizeBytes.Length);
        offset += fullMessageSizeBytes.Length;

        return offset;
    }

    /// <summary>
    ///    Serialize a ROS message in the expected format of
    ///     https://github.com/Unity-Technologies/Robotics-Tutorials/tree/master/catkin_ws/src/tcp_endpoint
    ///
    /// 	All messages are expected to come in the format of:
    /// 		first four bytes: int32 of the length of following string value
    /// 		next N bytes determined from previous four bytes: ROS topic name as a string
    /// 		next four bytes: int32 of the length of the remaining bytes for the ROS Message
    /// 		last N bytes determined from previous four bytes: ROS Message variables
    /// </summary>
    /// <param name="topicServiceName"></param> The ROS topic or service name that is receiving the messsage
    /// <param name="messsage"></param> The ROS message to send to a ROS publisher or service
    /// <returns> byte array with serialized ROS message in appropriate format</returns>
    public byte[] GetMessageBytes(string topicServiceName, Message message)
    {
        byte[] topicName = message.SerializeString(topicServiceName);
        byte[] bytesMsg = message.Serialize();
        byte[] fullMessageSizeBytes = BitConverter.GetBytes(bytesMsg.Length);

        byte[] messageBuffer = new byte[topicName.Length + fullMessageSizeBytes.Length + bytesMsg.Length];
        // Copy topic name and message size in bytes to message buffer
        int offset = GetPrefixBytes(0, topicName, fullMessageSizeBytes, messageBuffer);
        // ROS message bytes
        System.Buffer.BlockCopy(bytesMsg, 0, messageBuffer, offset, bytesMsg.Length);

        return messageBuffer;
    }

    protected void Connect()
    {
        lock(_connectionLock)
        {
            if (persistantClient == null || !persistantClient.Connected)
            {
                persistantClient = new TcpClient();            
                Debug.Log("Connecting ...");
                persistantClient.Connect(hostName, hostPort);
                persistantNetworkStream = persistantClient.GetStream();
                persistantNetworkStream.ReadTimeout = networkTimeout;
                Debug.Log("Connected");
            }
        }
    }
    
    public void Dispose()
    {        
        if (persistantClient.Connected)
            persistantClient.Close();
    }

    public async void Send(string rosTopicName, Message message)
    {
        if (!running)
            return;
        try
        {
            // Serialize the message in topic name, message size, and message bytes format
            byte[] messageBytes = GetMessageBytes(rosTopicName, message);
            if (keepConnection)
            {
                Connect();
                persistantNetworkStream.Write(messageBytes, 0, messageBytes.Length);
            } else
            {
                TcpClient client = new TcpClient();
                await client.ConnectAsync(hostName, hostPort);
                NetworkStream networkStream = client.GetStream();
                networkStream.ReadTimeout = networkTimeout;
                networkStream.Write(messageBytes, 0, messageBytes.Length);
                if (client.Connected)
                    client.Close();
            }
        }
        catch (NullReferenceException e)
        {
            Debug.LogError("TCPConnector.SendMessage Null Reference Exception: " + e);
        }
        catch (Exception e)
        {
            Debug.LogError("TCPConnector Exception: " + e);
        }
    }

    public async void SendServiceMessage<RESPONSE>(string rosServiceName, Message serviceRequest, Action<RESPONSE> callback) where RESPONSE : Message, new()
    {
        // Serialize the message in service name, message size, and message bytes format
        byte[] messageBytes = GetMessageBytes(rosServiceName, serviceRequest);

        NetworkStream networkStream = null;
        TcpClient client = null;
        if (keepConnection)
        {
            Connect();
            client = persistantClient;
            networkStream = persistantNetworkStream;
        } else
        {
            client = new TcpClient();
            await client.ConnectAsync(hostName, hostPort);
            networkStream = client.GetStream();
            networkStream.ReadTimeout = networkTimeout;
        }
        RESPONSE serviceResponse = new RESPONSE();

        // Send the message
        try
        {
            networkStream.Write(messageBytes, 0, messageBytes.Length);
        }
        catch (Exception e)
        {
            Debug.LogError("SocketException: " + e);
            goto finish;
        }

        if (!networkStream.CanRead)
        {
            Debug.LogError("Sorry, you cannot read from this NetworkStream.");
            goto finish;
        }

        // Poll every 1 second(s) for available data on the stream
        int attempts = 0;
        while (!networkStream.DataAvailable && attempts <= this.awaitDataMaxRetries)
        {
            if (attempts == this.awaitDataMaxRetries)
            {
                Debug.LogError("No data available on network stream after " + awaitDataMaxRetries + " attempts.");
                goto finish;
            }
            attempts++;
            await Task.Delay((int)(awaitDataSleepSeconds * 1000));
        }

        int numberOfBytesRead = 0;
        try
        {
            // Get first bytes to determine length of service name
            byte[] rawServiceBytes = new byte[4];
            networkStream.Read(rawServiceBytes, 0, rawServiceBytes.Length);
            int topicLength = BitConverter.ToInt32(rawServiceBytes, 0);

            // Create container and read service name from network stream
            byte[] serviceNameBytes = new byte[topicLength];
            networkStream.Read(serviceNameBytes, 0, serviceNameBytes.Length);
            string serviceName = Encoding.ASCII.GetString(serviceNameBytes, 0, topicLength);

            // Get leading bytes to determine length of remaining full message
            byte[] full_message_size_bytes = new byte[4];
            networkStream.Read(full_message_size_bytes, 0, full_message_size_bytes.Length);
            int full_message_size = BitConverter.ToInt32(full_message_size_bytes, 0);

            // Create container and read message from network stream
            byte[] readBuffer = new byte[full_message_size];
            while (networkStream.DataAvailable && numberOfBytesRead < full_message_size)
            {
                var readBytes = networkStream.Read(readBuffer, 0, readBuffer.Length);
                numberOfBytesRead += readBytes;
            }

            serviceResponse.Deserialize(readBuffer, 0);
        }
        catch (Exception e)
        {
            Debug.LogError("Exception raised!! " + e);
        }

        finish:
        callback(serviceResponse);
        if (!keepConnection && client.Connected)
            client.Close();
    }
}
