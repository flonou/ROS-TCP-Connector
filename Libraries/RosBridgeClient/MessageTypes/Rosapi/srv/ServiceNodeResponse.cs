/* 
 * This message is auto generated by ROS#. Please DO NOT modify.
 * Note:
 * - Comments from the original code will be written in their own line 
 * - Variable sized arrays will be initialized to array of size 0 
 * Please report any issues at 
 * <https://github.com/siemens/ros-sharp> 
 */

namespace RosSharp.RosBridgeClient.MessageTypes.Rosapi
{
    public class ServiceNodeResponse : Message
    {
        public const string RosMessageName = "rosapi/ServiceNode";

        public string node { get; set; }

        public ServiceNodeResponse()
        {
            this.node = "";
        }

        public ServiceNodeResponse(string node)
        {
            this.node = node;
        }
    }
}
