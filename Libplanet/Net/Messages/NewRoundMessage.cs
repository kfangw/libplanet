using System.Collections.Generic;
using NetMQ;

namespace Libplanet.Net.Messages
{
    public class NewRoundMessage : Message
    {
        public NewRoundMessage(int round)
        {
            Round = round;
        }

        public NewRoundMessage(NetMQFrame[] frames)
        {
            Round = frames[0].ConvertToInt32();
        }

        public int Round { get; set; }

        protected override MessageType Type => MessageType.ConsensusNewRound;

        protected override IEnumerable<NetMQFrame> DataFrames
        {
            get
            {
                yield return new NetMQFrame(
                        NetworkOrderBitsConverter.GetBytes(Round));
            }
        }
    }
}