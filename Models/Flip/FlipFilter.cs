
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Coflnet.Sky.Filter;
using hypixel;

namespace Coflnet.Sky.Commands.Shared
{
    public class FlipFilter
    {
        private static FilterEngine FilterEngine = new FilterEngine();

        private Func<SaveAuction, bool> Filters;
        private Func<FlipInstance, bool> FlipFilters = null;

        public static CamelCaseNameDictionary<DetailedFlipFilter> AdditionalFilters {private set; get; }= new();

        static FlipFilter()
        {
            AdditionalFilters.Add<VolumeDetailedFlipFilter>();
            AdditionalFilters.Add<ProfitDetailedFlipFilter>();
            AdditionalFilters.Add<ProfitPercentageDetailedFlipFilter>();
            AdditionalFilters.Add<FlipFinderDetailedFlipFilter>();
            AdditionalFilters.Add<MinProfitPercentageDetailedFlipFilter>();
            AdditionalFilters.Add<MinProfitDetailedFlipFilter>();
        }

        public FlipFilter(Dictionary<string, string> filters)
        {
            Expression<Func<FlipInstance, bool>> expression = null;
            if (filters != null)
                foreach (var item in AdditionalFilters.Keys)
                {
                    var match = filters.Where(f=>f.Key.ToLower() == item.ToLower()).FirstOrDefault();
                    if (match.Key != default)
                    {
                        filters.Remove(match.Key);
                        expression = AdditionalFilters[item].GetExpression(filters, match.Value);
                        Console.WriteLine("set expression " + expression.ToString());
                    }
                }
            Filters = FilterEngine.GetMatcher(filters);
            if (expression != null)
                FlipFilters = expression.Compile();
        }

        public bool IsMatch(FlipInstance flip)
        {
            return Filters == null || Filters(flip.Auction) && (FlipFilters == null || FlipFilters(flip));
        }

        public Expression<Func<FlipInstance, bool>> GetExpression()
        {
            if (Filters == null && FlipFilters == null)
                return f => true;
            if (FlipFilters == null)
                return f => Filters(f.Auction);
            if (Filters == null)
                return flip => FlipFilters(flip);
            return flip => Filters(flip.Auction) && FlipFilters(flip);
        }
    }

}