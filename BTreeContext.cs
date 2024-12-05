using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HeapsAndBTrees
{
    internal interface IVirtualNode<T, TT, TKey> 
        where T : IActualNode<TT, TKey>
        where TT : INode<TKey>
        where TKey : IComparable
    {
        public T Node { get; }
        public bool IsDirty { get; }
    }

    internal interface IActualNode<T, TKey>
        where T : INode<TKey>
        where TKey : IComparable
    { 
    
    }

    internal class BTreeContext<TKey, TValue, TNode, TVirtual, TActual> : IDisposable 
        where TKey: IComparable
        where TNode : INode<TKey>
        where TActual : IActualNode<TNode, TKey>, TNode
        where TVirtual : IVirtualNode<TActual, TNode, TKey>, TNode
    {
        private List<TVirtual> _bufferedNodes = new List<TVirtual>();
        private int _bufferSize;

        public TVirtual Root { get; protected set; }
        private BTreeOperation _operation;
        private IBTree<TKey, TValue, TNode, TVirtual, TActual> _tree;

        private Action<TNode, BTreeOperation> _read;
        private Action<TNode, BTreeOperation> _write;

        private Func<TActual, BTreeContext<TKey, TValue, TNode, TVirtual, TActual>, TVirtual> _wrapperFactory;

        private void UpdateRoot(TActual root)
        {
            Root = _wrapperFactory(root, this);
        }

        public BTreeContext(IBTree<TKey, TValue, TNode, TVirtual, TActual> tree,
            Action<TNode, BTreeOperation> read, Action<TNode, BTreeOperation> write,
            Func<TActual, BTreeContext<TKey, TValue, TNode, TVirtual, TActual>, TVirtual> wrapperFactory,
            TActual root, BTreeOperation operation, int bufferSize)
        {
            _read = read;
            _write = write;
            _wrapperFactory = wrapperFactory;
            _bufferSize = bufferSize;
            _bufferedNodes = new List<TVirtual>();
            _operation = operation;

            UpdateRoot(root);
            _tree = tree;
            _tree.OnRootChanged += UpdateRoot;
        }

        private void Pop()
        {
            var node = _bufferedNodes[0];
            if (node.IsDirty)
            {
                _write(node.Node, _operation);
            }

            _bufferedNodes.RemoveAt(0);
        }

        private void Push(TVirtual virtualNode)
        {
            _read(virtualNode.Node, _operation);
            _bufferedNodes.Add(virtualNode);
        }

        private bool IsLoaded(TVirtual node) => node as object == Root as object || _bufferedNodes.Contains(node);

        public void Actualize(TVirtual virtualNode)
        {
            if (IsLoaded(virtualNode))
            {
                return;
            }

            if (_bufferedNodes.Count == _bufferSize)
            {
                Pop();
            }

            Push(virtualNode);
        }

        private void Commit()
        {
            while (_bufferedNodes.Count != 0)
            {
                Pop();
            }
        }

        public void Dispose()
        {
            _tree.OnRootChanged -= UpdateRoot;
            Commit();
        }
    }
}
