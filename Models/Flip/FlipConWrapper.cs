using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Coflnet.Sky;
using Coflnet.Sky.Core;

namespace Coflnet.Sky.Commands.Shared
{
    public class FlipConWrapper
    {
        public IFlipConnection Connection;

        private Channel<LowPricedAuction> LowPriced = Channel.CreateBounded<LowPricedAuction>(50);

        private CancellationTokenSource cancellationTokenSource = null;
        private bool stopWrites;

        public bool Closed => stopWrites;

        public int ChannelCount => LowPriced.Reader.Count;

        public FlipConWrapper(IFlipConnection connection)
        {
            Connection = connection;
        }

        public void StartWorkers()
        {
            cancellationTokenSource?.Cancel();
            cancellationTokenSource = new CancellationTokenSource();
            var stoppingToken = cancellationTokenSource.Token;
            var count = Connection.AccountInfo?.Tier switch
            {
                AccountTier.PREMIUM => 3,
                AccountTier.PREMIUM_PLUS => 6,
                AccountTier.SUPER_PREMIUM => 12,
                _ => 1
            };
            LowPriced = Channel.CreateBounded<LowPricedAuction>(
                new BoundedChannelOptions(count * 80) { FullMode = BoundedChannelFullMode.DropWrite });
            var limiter = new SemaphoreSlim(count);

            for (int i = 0; i < count; i++)
            {
                var index = i;
                _ = Task.Run(async () =>
                {
                    Console.Write("Started worker " + index);
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        try
                        {
                            var flip = await LowPriced.Reader.ReadAsync(stoppingToken);
                            if (LowPriced.Reader.Count > 90)
                            {
                                Connection.Log("amany flips waiting " + LowPriced.Reader.Count, Microsoft.Extensions.Logging.LogLevel.Error);
                                flip.AdditionalProps?.TryAdd("long wait", LowPriced.Reader.Count.ToString());
                            }
                            var batch = new List<LowPricedAuction>();
                            AddCopyOfFlipToBatch(flip, batch);
                            while (batch.Count < 20 && LowPriced.Reader.TryRead(out flip))
                            {
                                AddCopyOfFlipToBatch(flip, batch);
                            }
                            //await limiter.WaitAsync();
                            await Connection.SendBatch(batch);
                        }
                        catch (OperationCanceledException)
                        {
                            Console.WriteLine($"worker {index} was canceled");
                            break;
                        }
                        catch (ChannelClosedException)
                        {
                            break;
                        }
                        catch (Exception e)
                        {
                            Connection.Log(e.ToString(), Microsoft.Extensions.Logging.LogLevel.Error);
                            dev.Logger.Instance.Error(e, "seding flip to " + Connection.UserId);
                        }
                    }
                }).ConfigureAwait(false);
            }
        }

        private static void AddCopyOfFlipToBatch(LowPricedAuction flip, List<LowPricedAuction> batch)
        {
            var copy = Copy(flip);
            copy.AdditionalProps["da"] = (DateTime.UtcNow - flip.Auction.FindTime).ToString();
            batch.Add(copy);
        }

        public bool AddLowPriced(LowPricedAuction lp)
        {
            if (stopWrites)
                return false;
            return LowPriced.Writer.TryWrite(lp);
        }

        private static LowPricedAuction Copy(LowPricedAuction lp)
        {
            return new LowPricedAuction()
            {
                AdditionalProps = lp.AdditionalProps == null ? new Dictionary<string, string>() : new Dictionary<string, string>(lp.AdditionalProps),
                Auction = lp.Auction,
                DailyVolume = lp.DailyVolume,
                Finder = lp.Finder,
                TargetPrice = lp.TargetPrice
            };
        }

        public Task<bool> SendFlip(FlipInstance flip)
        {
            return Connection.SendFlip(flip);
        }

        public void Stop()
        {
            stopWrites = true;
            cancellationTokenSource?.Cancel();
            if (LowPriced.Writer.TryComplete())
                while (LowPriced.Reader.Count > 0)
                    LowPriced.Reader.TryRead(out _);
            Connection.Log("canceled by " + Environment.StackTrace);
        }
    }
}
