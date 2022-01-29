
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using hypixel;

namespace Coflnet.Sky.Commands.Shared
{
    public class MinProfitDetailedFlipFilter : DetailedFlipFilter
    {
        public  Expression<Func<FlipInstance, bool>> GetExpression(Dictionary<string, string> filters, string val)
        {
            var min = long.Parse(val);
            return flip => flip.Profit > min;
        }
    }
    

}