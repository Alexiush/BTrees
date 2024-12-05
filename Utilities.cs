using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HeapsAndBTrees
{
    internal static class Utilities
    {
        public static void ShiftArrayLeft<T>(ref T[] array, int index, int actualLength)
        {
            T elementToMove = array[actualLength];
            for (int i = actualLength - 1; i >= index; i--)
            {
                T temp = array[i];
                array[i] = elementToMove;
                elementToMove = temp;
            }
        }

        public static void ShiftArrayRight<T>(ref T[] array, int index, int actualLength)
        {
            T elementToMove = array[index];
            for (int i = index + 1; i <= actualLength; i++)
            {
                T temp = array[i];
                array[i] = elementToMove;
                elementToMove = temp;
            }
        }
    }
}
