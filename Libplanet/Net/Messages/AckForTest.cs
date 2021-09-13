using System.Collections.Generic;
using NetMQ;

namespace Libplanet.Net.Messages
{
    public class AckForTest : Message
    {
        public AckForTest(byte[] payload)
        {
            Payload = payload;
        }

        public byte[] Payload { get; }

        protected override MessageType Type => MessageType.ConsensusAck;

        protected override IEnumerable<NetMQFrame> DataFrames
        {
            get
            {
                yield return new NetMQFrame(Payload);
            }
        }
    }
}