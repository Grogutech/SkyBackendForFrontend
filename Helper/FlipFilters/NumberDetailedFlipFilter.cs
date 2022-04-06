
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Coflnet.Sky.Filter;
using Coflnet.Sky.Core;

namespace Coflnet.Sky.Commands.Shared
{
    public class NumberDetailedFlipFilter : DetailedFlipFilter
    {
        public virtual object[] Options => new object[]{1,100_000_000};
        public FilterType FilterType => FilterType.NUMERICAL | FilterType.LOWER | FilterType.RANGE;
        public Expression<Func<FlipInstance, bool>> GetExpression(Dictionary<string, string> filters, string content)
        {
            var selector = GetSelector();
            if (content.Contains("-"))
            {
                var parts = content.Split("-").Select(a => double.Parse(a)).ToArray();
                var min = parts[0];
                var max = parts[1];
                return ExpressionMinMax(selector, min, max);
            }
            var value = double.Parse(content.Replace("<", "").Replace(">", ""));
            Console.WriteLine(value);
            if (content.StartsWith("<"))
                return ExpressionMinMax(selector, 1, value - 1);
            if (content.StartsWith(">"))
            {
                return ExpressionMinMax(selector, value, int.MaxValue);
            }

            return ExpressionMinMax(selector, value, value);
            //return flip => flip.ProfitPercentage > min;
        }

        protected virtual Expression<Func<FlipInstance, double>> GetSelector()
        {
            return (f) => (double)f.Volume;
        }


        public static Expression<Func<T, bool>> ExpressionMinMax<T>(Expression<Func<T, double>> selector, double min, double max)
        {
            return Expression.Lambda<Func<T, bool>>(
                Expression.And(
                    Expression.GreaterThanOrEqual(
                    Expression.Constant(max, typeof(double)),
                    selector.Body),
                    Expression.GreaterThanOrEqual(
                    selector.Body,
                    Expression.Constant(min, typeof(double))
                )), selector.Parameters
            );
        }
    }
    

}