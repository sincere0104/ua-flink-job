package bitmap;

/**
 * @ClassName BinaryOperate
 * @Description TODO
 * @Author zby
 * @Date 2021-12-15 15:51
 * @Version 1.0
 **/
public class BinaryOperate {
    public static void main(String[] args) {
        // 若最高的几位为0则不输出这几位，从为1的那一位开始输出
        //                            1010
        //11111111111111111111111111110110
        System.out.println(Integer.toBinaryString(10));
        System.out.println(Integer.toBinaryString(-10));
        System.out.println(Integer.toBinaryString(7));
        System.out.println(Integer.toBinaryString(2));


        int[] BIT_VALUE = {0x00000001, 0x00000002, 0x00000004, 0x00000008, 0x00000010, 0x00000020,
                0x00000040, 0x00000080, 0x00000100, 0x00000200, 0x00000400, 0x00000800, 0x00001000, 0x00002000, 0x00004000,
                0x00008000, 0x00010000, 0x00020000, 0x00040000, 0x00080000, 0x00100000, 0x00200000, 0x00400000, 0x00800000,
                0x01000000, 0x02000000, 0x04000000, 0x08000000, 0x10000000, 0x20000000, 0x40000000, 0x80000000};


        System.out.println(BIT_VALUE.length);

        for (int i : BIT_VALUE) {
            System.out.println(i);
        }


    }
}
