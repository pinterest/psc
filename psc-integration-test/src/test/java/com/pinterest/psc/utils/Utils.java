package com.pinterest.psc.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class Utils {
    public static <T> Set<T> getRandomElements(List<T> list, int count) throws Exception {
        if (count < 0) return null;
        if (count == 0) return new HashSet<>();
        int size = list.size();
        if (count > size)
            throw new Exception(String.format("count > list size: %d > %d", count, size));

        Set<Integer> randomIndices = new HashSet<>();
        Random random = new Random();
        while (randomIndices.size() < count)
            randomIndices.add(random.nextInt(size));

        Set<T> randomElements = new HashSet<>();
        randomIndices.forEach(index -> randomElements.add(list.get(index)));
        return randomElements;
    }

    public static Set<Integer> getRandomNumbers(int min, int max, int count) throws Exception {
        List<Integer> list = new ArrayList<>();
        for (int i = min; i <= max; ++i)
            list.add(i);
        return getRandomElements(list, count);
    }
}
