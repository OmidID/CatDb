namespace CatDb.General.Threading
{
    public class Countdown
    {
        private long count; 
        
        public void Increment()
        {
            Interlocked.Increment(ref count);
        }

        public void Decrement()
        {
            Interlocked.Decrement(ref count);
        }

        public void Wait()
        {
            var wait = new SpinWait();

            wait.SpinOnce();

            while (Count > 0)
                Thread.Sleep(1);
        }

        public long Count => Interlocked.Read(ref count);
    }
}
