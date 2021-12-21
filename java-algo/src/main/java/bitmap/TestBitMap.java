package bitmap;

/**
 * @ClassName TestBitMap
 * @Description TODO
 * @Author zby
 * @Date 2021-12-15 15:33
 * @Version 1.0
 **/
public class TestBitMap {

    public static void main(String[] args) {
        int n = 100;
        new TestBitMap().create(n);
    }

    public byte[] create(int n) {

        byte[] bits = new byte[getIndex(n) + 1];

        System.out.println("byte数组的长度：" + bits.length);

        for (int i = 0; i < n; i++) {
            add(bits, i);
        }
        //查看数字 11 是否存在
        System.out.println(contains(bits, 11));

        int index = 1;
        for (byte bit : bits) {
            //System.out.println("-------" + index++ + "-------");
            showByte(bit);
        }
        return bits;
    }

    /**
     * 打印byte类型的变量
     * 将byte转换为一个长度为8的byte数组，数组每个值代表bit
     * @param b
     */
    private void showByte(byte b) {
        byte[] array = new byte[8];
        for (int i = 7; i >= 0; i--) {
            array[i] = (byte)(b & 1);
            b = (byte)(b >> 1);
        }

        for (byte b1 : array) {
            System.out.print(b1 + " ");
        }

        System.out.println("");
    }

    /**
     * 判断指定数字num是否存在
     * 将1左移position后，那个位置自然就是1，然后和以前的数据做&，判断是否为0即可
     * @param bits
     * @param num
     * @return
     */
    public boolean contains(byte[] bits, int num) {
        return (bits[getIndex(num)] & (1 << getPosition(num))) != 0;
    }

    /**
     * num/8得到byte[]的index
     * @param num
     * @return
     */
    private int getIndex(int num) {
        return num >> 3;
    }

    /**
     * 标记指定数字（num）在bitmap中的值，标记其已经出现过
     * 将1左移position后，那个位置自然就是1，然后和以前的数据做|，这样，那个位置就替换成1了
     * @param bits
     * @param num
     */
    public void add(byte[] bits, int num) {
        bits[getIndex(num)] |= 1 << getPosition(num);
    }

    /**
     * num%8得到在byte[index]的位置
     * @param num
     * @return
     */
    private int getPosition(int num) {
        return num & 0x07;
    }
}

