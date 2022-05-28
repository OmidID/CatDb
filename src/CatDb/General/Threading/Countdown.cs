namespace CatDb.General.Threading
{
    public class Countdown
    {
        private long _count; 
        
        public void Increment()
        {
            Interlocked.Increment(ref _count);
        }

        public void Decrement()
        {
            Interlocked.Decrement(ref _count);
        }

        public void Wait()
        {
            var wait = new SpinWait();

            wait.SpinOnce();

            while (Count > 0)
                Thread.Sleep(1);
        }

        public long Count => Interlocked.Read(ref _count);
    }
}
