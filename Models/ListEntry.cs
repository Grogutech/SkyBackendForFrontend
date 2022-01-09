
using System.Collections.Generic;
using System.Runtime.Serialization;
using hypixel;

namespace Coflnet.Sky.Commands.Shared
{
    [DataContract]
    public class ListEntry
    {
        [DataMember(Name = "tag")]
        public string ItemTag;
        [DataMember(Name = "filter")]
        public Dictionary<string, string> filter;

        private FlipFilter filterCache;

        public bool MatchesSettings(FlipInstance flip)
        {
            if (filterCache == null)
                filterCache = new FlipFilter(this.filter);
            return (ItemTag == null || ItemTag == flip.Auction.Tag ) &&  filterCache.IsMatch(flip);
        }
    }
}