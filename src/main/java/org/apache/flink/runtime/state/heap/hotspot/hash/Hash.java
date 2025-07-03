package org.apache.flink.runtime.state.heap.hotspot.hash;

public class Hash {
    private int prime32Num;

    // C++ 中定义的最大素数列表长度
    private static final int MAX_PRIME32 = 1229;

    private static final int[] prime32 = new int[] {
            2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,
            73,79,83,89,97,101,103,107,109,113,127,131,137,139,149,151,157,163,167,173,
            179,181,191,193,197,199,211,223,227,229,233,239,241,251,257,263,269,271,277,281,
            283,293,307,311,313,317,331,337,347,349,353,359,367,373,379,383,389,397,401,409,
            419,421,431,433,439,443,449,457,461,463,467,479,487,491,499,503,509,521,523,541,
            547,557,563,569,571,577,587,593,599,601,607,613,617,619,631,641,643,647,653,659,
            661,673,677,683,691,701,709,719,727,733,739,743,751,757,761,769,773,787,797,809,
            811,821,823,827,829,839,853,857,859,863,877,881,883,887,907,911,919,929,937,941,
            947,953,967,971,977,983,991,997,1009,1013,1019,1021,1031,1033,1039,1049,1051,1061,
            1063,1069,1087,1091,1093,1097,1103,1109,1117,1123,1129,1151,1153,1163,1171,1181,1187,1193,1201,1213,
            1217,1223,1229
    };

    public Hash() {
        // 默认选择第 0 个素数
        this.prime32Num = 0;
    }

    /**
     * 指定 prime32 索引，用于多实例化
     */
    public Hash(int prime32Num) {
        this.prime32Num = prime32Num % prime32.length;
    }

    public void initialize(int prime32Num) {
        this.prime32Num = prime32Num % prime32.length;
    }

    /**
     * 计算 data[0..len) 的 32 位 BobHash2
     */
    public int run(byte[] data, int len) {
        int a = 0x9e3779b9;
        int b = 0x9e3779b9;
        int c = prime32[prime32Num];

        int i = 0;
        while (i + 12 <= len) {
            a += ((data[i]   & 0xFF)) |
                    ((data[i+1] & 0xFF) << 8) |
                    ((data[i+2] & 0xFF) << 16) |
                    ((data[i+3] & 0xFF) << 24);
            b += ((data[i+4] & 0xFF)) |
                    ((data[i+5] & 0xFF) << 8) |
                    ((data[i+6] & 0xFF) << 16) |
                    ((data[i+7] & 0xFF) << 24);
            c += ((data[i+8] & 0xFF)) |
                    ((data[i+9] & 0xFF) << 8) |
                    ((data[i+10]& 0xFF)<< 16) |
                    ((data[i+11]& 0xFF)<< 24);

            // mix
            a -= b; a -= c; a ^= (c >>> 13);
            b -= c; b -= a; b ^= (a << 8);
            c -= a; c -= b; c ^= (b >>> 13);
            a -= b; a -= c; a ^= (c >>> 12);
            b -= c; b -= a; b ^= (a << 16);
            c -= a; c -= b; c ^= (b >>> 5);
            a -= b; a -= c; a ^= (c >>> 3);
            b -= c; b -= a; b ^= (a << 10);
            c -= a; c -= b; c ^= (b >>> 15);

            i += 12;
        }

        int remaining = len - i;
        c += remaining;
        switch (remaining) {
            case 11: c += (data[i+10] & 0xFF) << 24;
            case 10: c += (data[i+9]  & 0xFF) << 16;
            case 9 : c += (data[i+8]  & 0xFF) << 8;
            case 8 : b += (data[i+7]  & 0xFF) << 24;
            case 7 : b += (data[i+6]  & 0xFF) << 16;
            case 6 : b += (data[i+5]  & 0xFF) << 8;
            case 5 : b += (data[i+4]  & 0xFF);
            case 4 : a += (data[i+3]  & 0xFF) << 24;
            case 3 : a += (data[i+2]  & 0xFF) << 16;
            case 2 : a += (data[i+1]  & 0xFF) << 8;
            case 1 : a += (data[i]    & 0xFF);
        }

        // final mix
        c ^= b; c -= (b << 14) | (b >>> 18);
        a ^= c; a -= (c << 11) | (c >>> 21);
        b ^= a; b -= (a << 25) | (a >>> 7);
        c ^= b; c -= (b << 16) | (b >>> 16);
        a ^= c; a -= (c << 4)  | (c >>> 28);
        b ^= a; b -= (a << 14) | (a >>> 18);
        c ^= b; c -= (b << 24) | (b >>> 8);

        return c;
    }
}
