using System;
using StackExchange.Redis;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;

namespace Coflnet.Sky.Commands.Shared
{
    public class SelfUpdatingValue<T> : IDisposable
    {
        public T Value { get; private set; }
        public event Action<T> OnChange;
        public event Action<T> AfterChange;
        private string UserId;
        private string Key;
        private bool IsDisposed = false;

        ChannelMessageQueue subTask;

        private SelfUpdatingValue(string userId, string key)
        {
            this.UserId = userId;
            this.Key = key;
        }

        public static async Task<SelfUpdatingValue<T>> Create(string userId, string key, Func<T> defaultGetter = null)
        {
            var instance = new SelfUpdatingValue<T>(userId, key);
            SettingsService settings = GetService();
            instance.subTask = await settings.GetAndSubscribe<T>(userId, key, v =>
            {
                instance.OnChange?.Invoke(v);
                instance.Value = v;
                instance.AfterChange?.Invoke(v);
            }, defaultGetter);
            // if instance is already disposed we need to unsubscribe, this may or may not be bullshit
            if(instance.IsDisposed)
                instance.Dispose();
            return instance;
        }

        public static Task<SelfUpdatingValue<T>> CreateNoUpdate(Func<T> valGet)
        {
            var instance = new SelfUpdatingValue<T>(null, null);
            instance.Value = valGet();
            return Task.FromResult(instance);
        }
        public static SelfUpdatingValue<T> CreateNoUpdate(T value)
        {
            var instance = new SelfUpdatingValue<T>(null, null);
            instance.Value = value;
            return instance;
        }

        private static SettingsService GetService()
        {
            return DiHandler.ServiceProvider.GetRequiredService<SettingsService>();
        }

        public void Dispose()
        {
            IsDisposed = true;
            subTask?.Unsubscribe();
            subTask = null;
        }

        /// <summary>
        /// Replaces the current value with a new one
        /// </summary>
        /// <param name="newValue"></param>
        /// <returns></returns>
        public async Task Update(T newValue)
        {
            if(UserId == null)
            {
                Value = newValue;
                return;
            }
            await GetService().UpdateSetting(UserId, Key, newValue);
        }

        /// <summary>
        /// Updates the current value by applying the given action
        /// tires to lock the value
        /// </summary>
        public async Task Update(Action<T> update)
        {
            update(Value);
            await Update(Value);
        }

        /// <summary>
        /// Updates the current value
        /// </summary>
        /// <returns></returns>
        public async Task Update()
        {
            await Update(Value);
        }

        public static implicit operator T(SelfUpdatingValue<T> val) => val == null ? default(T) : val.Value;
    }
}
