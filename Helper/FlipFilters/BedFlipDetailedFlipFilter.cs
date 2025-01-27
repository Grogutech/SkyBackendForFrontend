
using System;
using System.Linq.Expressions;

namespace Coflnet.Sky.Commands.Shared
{
    public class BedFlipDetailedFlipFilter : BoolDetailedFlipFilter
    {
        public override Expression<Func<FlipInstance, bool>> GetStateExpression(bool expected)
        {
            return flip => (flip.Auction.Start + TimeSpan.FromSeconds(20) > DateTime.Now) == expected;
        }
    }
}