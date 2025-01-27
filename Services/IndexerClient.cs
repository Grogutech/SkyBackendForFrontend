using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using RestSharp;

namespace Coflnet.Sky.Commands.Shared
{
    public class IndexerClient
    {
        public static RestClient Client = new RestClient(SimplerConfig.SConfig.Instance["INDEXER_BASE_URL"] ?? "http://" + SimplerConfig.SConfig.Instance["INDEXER_HOST"]);
        public static Task<RestSharp.RestResponse<Player>> TriggerNameUpdate(string uuid)
        {
            return Client.ExecuteAsync<Player>(new RestRequest("player/{uuid}", Method.Patch).AddUrlSegment("uuid", uuid));
        }
        public static async Task<IEnumerable<KeyValuePair<string, short>>> LowSupply()
        {
            var response = await Client.ExecuteAsync(new RestRequest("supply/low", Method.Get));
            return Newtonsoft.Json.JsonConvert.DeserializeObject<List<KeyValuePair<string, short>>>(response.Content);
        }
    }
}