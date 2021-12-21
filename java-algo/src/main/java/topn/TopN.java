package topn;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * @ClassName TopN
 * @Description TODO
 * @Author zby
 * @Date 2021-12-06 17:11
 * @Version 1.0
 **/
public class TopN {
    public static void main(String[] args) {

        List<Integer> integers = Arrays.asList(2, 3, 1, 7, 9, 10, 6, 8, 5, 11);

//        integers.sort(new Comparator<Integer>() {
//            @Override
//            public int compare(Integer o1, Integer o2) {
//                return -(o1 - o2);
//            }
//        });
        System.out.println(integers.toString());

        //System.out.println(integers.size());
        sortTopN(integers,new IntegerComparator(),5);
        System.out.println(11111);
        for (int i = 0; i < 5; i++) {
            System.out.println(integers.get(i));
        }

    }

    /**
     * 将前 N 个最大的元素，放到
     */
    private static <T> void sortTopN(List<T> list, Comparator<? super T> c, int N) {
        T[] array = (T[]) list.toArray();
        int L = 0;
        int R = list.size() - 1;
        while (L != R) {
            int partition = recSortTopN(array, L, R, c);
            // 第 N 个位置已经拍好序
            if (partition == N) {
                return;
            } else if (partition < N) {
                L = partition + 1;
            } else {
                R = partition - 1;
            }
        }
    }

    private static <E> int recSortTopN(E[] array, int L, int R,
                                       Comparator<? super E> c) {
        // 将 L mid R 三个位置的中位数的 index 返回，并将其交换到 L 的位置
        int mid = getMedian(array, L, R, c);
        if (mid != L) {
            swap(array, L, mid);
        }

        // 小的放左边，大的放右边
        E pivot = array[L];
        int i = L;
        int j = R;
        while (i < j) {
            // i 位置小于 等于 pivot，则 i 一直右移
            while (c.compare(array[i], pivot) <= 0 && i < j) {
                i++;
            }
            // j 位置大于 等于 pivot，则 j 一直左移
            while (c.compare(array[i], pivot) <= 0 && i < j) {
                j--;
            }
            if (i < j) {
                swap(array, i, j);
            }
        }
        swap(array, L, i);
        return i;
    }


    private static <E> void swap(E[] array, int i, int j) {
        E tmp = array[i];
        array[i] = array[j];
        array[j] = tmp;
    }


    private static <E> int getMedian(E[] array, int L, int R,
                                     Comparator<? super E> c) {
        int mid = L + ((R - L) >> 1);
        // 拿到三个元素的值，返回中间元素 的 index
        E valueL = array[L];
        E valueMid = array[mid];
        E valueR = array[R];
        if (c == null) {
            return 0;
        }
        if (c.compare(valueL, valueMid) <= 0 && c.compare(valueMid, valueR) <= 0) {
            return mid;
        } else if (c.compare(valueMid, valueL) <= 0 && c.compare(valueL, valueR) <= 0) {
            return L;
        } else {
            return R;
        }
    }

    private static class IntegerComparator implements Comparator<Integer> {
        @Override
        public int compare(Integer o1, Integer o2) {
            long diff = o2 - o1;
            return diff == 0 ? 0 : diff > 0 ? 1 : -1;
        }
    }
}
