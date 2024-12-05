using Optional;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HeapsAndBTrees
{
    interface IBTree<TKey, TValue, TNode, TVirtual, TActual>
        where TKey : IComparable
        where TNode : INode<TKey>
        where TActual : IActualNode<TNode, TKey>, TNode
        where TVirtual : IVirtualNode<TActual, TNode, TKey>, TNode
    {
        public Option<TValue> Search(TKey key);
        public void Insert(TKey key, TValue value);
        public void Delete(TKey key);
        public IEnumerable<(TKey, TValue)> Traverse(); 
        public void Clear();

        public delegate void OnRootChangedEvent(TActual root);
        public event OnRootChangedEvent OnRootChanged;
    }

    internal interface IDiagnostableBTree<TKey, TValue, TNode, TVirtual, TActual> : IBTree<TKey, TValue, TNode, TVirtual, TActual>
        where TKey : IComparable
        where TNode : INode<TKey>
        where TActual : IActualNode<TNode, TKey>, TNode
        where TVirtual : IVirtualNode<TActual, TNode, TKey>, TNode
    {
        public BTreeDiagnosticsData Retrieve(BTreeOperation operation);
        public void Watch();
        public void ResetWatch();
    }

    public enum BTreeOperation
    {
        Search = 0,
        Insert = 1,
        Delete = 2
    }

    public record BTreeDiagnosticsData
    {
        public int Count;
        public int Reads;
        public int Writes;

        public void Log()
        {
            Console.WriteLine($"Operations count: {Count}");
            Console.WriteLine($"Reads: {Reads} Reads (Avg): {(double)Reads / Count}");
            Console.WriteLine($"Writes: {Writes} Writes (Avg): {(double)Writes / Count}");
        }
    }

    internal abstract class DiagnostableBTreeBase<TKey, TValue, TNode, TVirtual, TActual> : IDiagnostableBTree<TKey, TValue, TNode, TVirtual, TActual>
        where TKey : IComparable
        where TNode : INode<TKey>
        where TActual : IActualNode<TNode, TKey>, TNode
        where TVirtual : IVirtualNode<TActual, TNode, TKey>, TNode
    {
        protected Dictionary<BTreeOperation, BTreeDiagnosticsData> _diagnosticsData = new Dictionary<BTreeOperation, BTreeDiagnosticsData>
        {
            { BTreeOperation.Search, new BTreeDiagnosticsData() },
            { BTreeOperation.Insert, new BTreeDiagnosticsData() },
            { BTreeOperation.Delete, new BTreeDiagnosticsData() },
        };
        protected bool _watched;

        public void Watch()
        {
            _watched = true;
        }

        public void ResetWatch()
        {
            _watched = false;

            _diagnosticsData.Clear();
            _diagnosticsData = new Dictionary<BTreeOperation, BTreeDiagnosticsData>
            {
                { BTreeOperation.Search, new BTreeDiagnosticsData() },
                { BTreeOperation.Insert, new BTreeDiagnosticsData() },
                { BTreeOperation.Delete, new BTreeDiagnosticsData() },
            };
        }

        public BTreeDiagnosticsData Retrieve(BTreeOperation operation)
        {
            return _diagnosticsData.GetValueOrDefault(operation, new BTreeDiagnosticsData());
        }

        public abstract Option<TValue> Search(TKey key);
        public abstract void Insert(TKey key, TValue value);
        public abstract void Delete(TKey key);
        public abstract void Clear();
        public abstract IEnumerable<(TKey, TValue)> Traverse();

        public event IBTree<TKey, TValue, TNode, TVirtual, TActual>.OnRootChangedEvent OnRootChanged;
        protected void InvokeOnRootChanged(TActual node) => OnRootChanged?.Invoke(node);
    }
}
