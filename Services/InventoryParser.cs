using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Coflnet.Sky.Core;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Coflnet.Sky.Commands.Shared;

public class InventoryParser
{
    /* json sample
    {
    "_events": {},
    "_eventsCount": 0,
    "id": 0,
    "type": "minecraft:inventory",
    "title": "Inventory",
    "slots": [
        null,
        null,
        null,
        null,
        null,
        {
            "type": 306,
            "count": 1,
            "metadata": 0,
            "nbt": {
                "type": "compound",
                "name": "",
                "value": {
                    "ench": {
                        "type": "list",
                        "value": {
                            "type": "end",
                            "value": []
                        }
                    },
                    "Unbreakable": {
                        "type": "byte",
                        "value": 1
                    },
                    "HideFlags": {
                        "type": "int",
                        "value": 254
                    },
                    "display": {
                        "type": "compound",
                        "value": {
                            "Lore": {
                                "type": "list",
                                "value": {
                                    "type": "string",
                                    "value": [
                                        "┬º7Defense: ┬ºa+10",
                                        "",
                                        "┬º7Growth I",
                                        "┬º7Grants ┬ºa+15 ┬ºcÔØñ Health┬º7.",
                                        "",
                                        "┬º7┬ºcYou do not have a high enough",
                                        "┬ºcEnchanting level to use some of",
                                        "┬ºcthe enchantments on this item!",
                                        "",
                                        "┬º7┬º8This item can be reforged!",
                                        "┬ºf┬ºlCOMMON HELMET"
                                    ]
                                }
                            },
                            "Name": {
                                "type": "string",
                                "value": "┬ºfIron Helmet"
                            }
                        }
                    },
                    "ExtraAttributes": {
                        "type": "compound",
                        "value": {
                            "id": {
                                "type": "string",
                                "value": "IRON_HELMET"
                            },
                            "enchantments": {
                                "type": "compound",
                                "value": {
                                    "growth": {
                                        "type": "int",
                                        "value": 1
                                    }
                                }
                            },
                            "mined_crops": {
                                "type": "long",
                                "value": [
                                    1,
                                    8314091
                                ]
                            },
                            "uuid": {
                                "type": "string",
                                "value": "0cf52647-c130-43ec-9c46-e2dc162d4894"
                            },
                            "timestamp": {
                                "type": "string",
                                "value": "2/18/23 4:27 AM"
                            }
                        }
                    }
                }
            },
            "name": "iron_helmet",
            "displayName": "Iron Helmet",
            "stackSize": 1,
            "slot": 5
        }
	  ]
}*/
    public IEnumerable<SaveAuction> Parse(string json)
    {
        dynamic full = JsonConvert.DeserializeObject(json);
        foreach (var item in full.slots)
        {
            if (item == null)
            {
                yield return null;
                continue;
            }

            var ExtraAttributes = item.nbt.value?.ExtraAttributes?.value;
            if (ExtraAttributes == null)
            {
                yield return new SaveAuction()
                {
                    Tag = "UNKOWN",
                    Enchantments = new(),
                    Count = 1,
                    ItemName = item.displayName,
                    Uuid = ExtraAttributes?.uuid?.value ?? Random.Shared.Next().ToString(),
                };
                continue;
            }
            Dictionary<string, object> attributesWithoutEnchantments = null;
            SaveAuction auction = null;
            try
            {
                NewMethod(item, ExtraAttributes, out attributesWithoutEnchantments, out auction);
                auction?.SetFlattenedNbt(NBT.FlattenNbtData(attributesWithoutEnchantments).GroupBy(e => e.Key).Select(e => e.First()).ToList());
            }
            catch (System.Exception e)
            {
                Activity.Current?.AddEvent(new ActivityEvent("Log", default, new(new Dictionary<string, object>() { {
                    "message", "Error while parsing inventory" }, { "error", e }, {"item", JsonConvert.SerializeObject(item)} })));
                dev.Logger.Instance.Error(e, "Error while parsing inventory");
            }
            yield return auction;
        }
    }

    private void NewMethod(dynamic item, dynamic ExtraAttributes, out Dictionary<string, object> attributesWithoutEnchantments, out SaveAuction auction)
    {
        attributesWithoutEnchantments = new Dictionary<string, object>();
        Denest(ExtraAttributes, attributesWithoutEnchantments);
        var enchantments = new Dictionary<string, int>();
        if (ExtraAttributes.enchantments?.value != null)
            foreach (var enchantment in ExtraAttributes.enchantments.value)
            {
                Console.WriteLine(enchantment.Value.value.GetType());
                var val = new Newtonsoft.Json.Linq.JValue(2);

                enchantments.Add(enchantment.Name, (int)enchantment.Value.value);
            }

        auction = new SaveAuction
        {
            Tag = ExtraAttributes.id.value,
            Enchantments = enchantments.Select(e => new Enchantment() { Type = Enum.Parse<Enchantment.EnchantmentType>(e.Key), Level = (byte)e.Value }).ToList(),
            Count = item.count,
            ItemName = item.displayName,
            Uuid = ExtraAttributes?.uuid?.value ?? Random.Shared.Next().ToString(),
        };
        if (attributesWithoutEnchantments.ContainsKey("modifier"))
        {
            auction.Reforge = Enum.Parse<ItemReferences.Reforge>(attributesWithoutEnchantments["modifier"].ToString(), true);
            attributesWithoutEnchantments.Remove("modifier");
        }
    }

    private static void Denest(dynamic ExtraAttributes, Dictionary<string, object> attributesWithoutEnchantments)
    {
        foreach (JProperty attribute in ExtraAttributes)
        {
            if (attribute.Name == "enchantments")
                continue;

            // p.Name
            var type = attribute.Value["type"].ToString();
            if (type == "list")
            {
                var list = new List<object>();
                foreach (var item in attribute.Value["value"]["value"])
                {
                    list.Add(item.ToString());
                }
                attributesWithoutEnchantments[attribute.Name] = list;
            }
            else if ((attribute.Name.EndsWith("_0") || attribute.Name.EndsWith("_1") || attribute.Name.EndsWith("_2") || attribute.Name.EndsWith("_3") || attribute.Name.EndsWith("_4"))
                        && type == "compound")
            {
                // has uuid
                var values = attribute.Value["value"];
                attributesWithoutEnchantments[attribute.Name] = values["quality"]["value"].ToString();
                attributesWithoutEnchantments[attribute.Name + ".uuid"] = values["uuid"].ToString();
            }
            else if (type == "compound")
                Denest(attribute.Value["value"], attributesWithoutEnchantments);
            else if (type == "long")
                attributesWithoutEnchantments[attribute.Name] = ((long)attribute.Value["value"][0] << 32) + (int)attribute.Value["value"][1];
            else
                attributesWithoutEnchantments[attribute.Name] = attribute.Value["value"];
        }
    }
}
