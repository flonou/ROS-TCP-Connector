/* 
 * This message is auto generated by ROS#. Please DO NOT modify.
 * Note:
 * - Comments from the original code will be written in their own line 
 * - Variable sized arrays will be initialized to array of size 0 
 * Please report any issues at 
 * <https://github.com/siemens/ros-sharp> 
 */



namespace RosSharp.RosBridgeClient.MessageTypes.ObjectRecognition
{
    public class ObjectRecognitionGoal : Message
    {
        public const string RosMessageName = "object_recognition_msgs/ObjectRecognitionGoal";

        //  Optional ROI to use for the object detection
        public bool use_roi { get; set; }
        public float[] filter_limits { get; set; }

        public ObjectRecognitionGoal()
        {
            this.use_roi = false;
            this.filter_limits = new float[0];
        }

        public ObjectRecognitionGoal(bool use_roi, float[] filter_limits)
        {
            this.use_roi = use_roi;
            this.filter_limits = filter_limits;
        }
    }
}
