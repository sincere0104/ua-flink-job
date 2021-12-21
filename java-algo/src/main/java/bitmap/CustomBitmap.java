package bitmap;

import java.util.Arrays;

/**
 * @ClassName CustomBitmap
 * @Description TODO
 * @Author zby
 * @Date 2021-12-15 15:21
 * @Version 1.0
 **/
public class CustomBitmap {
    private byte[] slots;
    private int size;
    private int slotCnt;
    private static final int STEPS_PER_SLOT = 3;
    private static final int BITS_PER_SLOT = 1 << STEPS_PER_SLOT;
    private static final int REMAINDER_TAG = 0x07;

    public CustomBitmap(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("The init size cannot be negative.");
        }
        this.size = size;
        initSlots(size);
    }

    private void initSlots(int size) {
        slotCnt = slotIndex(size - 1) + 1;
        slots = new byte[slotCnt];
    }

    private static int slotIndex(int bitIndex) {
        return bitIndex >> STEPS_PER_SLOT;
    }

    private static int indexInSlot(int bitIndex) {
        return bitIndex & REMAINDER_TAG;
    }

    public boolean valueAt(int offset) {
        if (offset < 0 || offset > this.size) {
            throw new IllegalArgumentException("Index out of bound.");
        }
        return (slots[slotIndex(offset)] & (1 << indexInSlot(offset))) != 0;
    }

    public void setValue(int offset, boolean value) {
        if (offset < 0 || offset > this.size) {
            throw new IllegalArgumentException("Index out of bound.");
        }
        if (value) {
            slots[slotIndex(offset)] |= (1 << indexInSlot(offset));
        } else {
            slots[slotIndex(offset)] &= ~(1 << indexInSlot(offset));
        }
    }

    public void and(CustomBitmap other) {
        if (this.size != other.size) {
            throw new IllegalArgumentException("Must have same size");
        }
        for (int i = 0; i < slotCnt; i++) {
            this.slots[i] &= other.slots[i];
        }
    }

    public void or(CustomBitmap other) {
        if (this.size != other.size) {
            throw new IllegalArgumentException("Must have same size");
        }
        for (int i = 0; i < slotCnt; i++) {
            this.slots[i] |= other.slots[i];
        }
    }

    public void xor(CustomBitmap other) {
        if (this.size != other.size) {
            throw new IllegalArgumentException("Must have same size");
        }
        for (int i = 0; i < slotCnt; i++) {
            this.slots[i] ^= other.slots[i];
        }
    }

    public int size() {
        return this.size;
    }

    public void clear() {
        Arrays.fill(slots, (byte) 0);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomBitmap bitMap = (CustomBitmap) o;
        return size == bitMap.size && Arrays.equals(slots, bitMap.slots);
    }

    @Override
    public String toString() {
        return "BitMap{" +
                "slots=" + Arrays.toString(slots) +
                ", size=" + size +
                '}';
    }
}
