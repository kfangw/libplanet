namespace Libplanet.DataBase
{
    public interface IIterator
    {
        public bool Valid();

        public void Next();

        public byte[] Key();

        public byte[] Value();
    }
}
