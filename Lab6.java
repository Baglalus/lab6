import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Lab6 {
    private static final int THREADS = 10;

    public static void main(String[] args) throws InterruptedException {
        if (args.length == 0) {
            System.out.println("Запуск: ./run.sh <1..11> [секунды для 2 или 11]");
            return;
        }
        int task = Integer.parseInt(args[0]);
        long seconds = args.length > 1 ? Long.parseLong(args[1]) : 0;
        if (task < 1 || task > 11 || seconds < 0 || args.length > 2
                || (args.length == 2 && task != 2 && task != 11)) {
            throw new IllegalArgumentException("Номер задания: 1..11; время: >= 0, только для 2 и 11");
        }
        switch (task) {
            case 1: hello(); break;
            case 2: timers(2, seconds); break;
            case 3: timers(3, 5); break;
            case 4: timers(4, 5); break;
            case 5: producersConsumers(new UnsafeBuffer(), false); break;
            case 6: producersConsumers(new LockBuffer(), true); break;
            case 7: producersConsumers(new MonitorBuffer(), true); break;
            case 8: pi(); break;
            case 9: philosophers(); break;
            case 10: readersWriters(); break;
            case 11: shower(seconds); break;
            default: throw new AssertionError(task);
        }
    }

    private static void startAll(List<Thread> threads) {
        for (Thread thread : threads) thread.start();
    }

    private static void joinAll(List<Thread> threads) throws InterruptedException {
        for (Thread thread : threads) thread.join();
    }

    private static void interruptAll(List<Thread> threads) {
        for (Thread thread : threads) thread.interrupt();
    }

    private static class HelloThread extends Thread {
        private final int number;

        HelloThread(int number) {
            this.number = number;
        }

        @Override
        public void run() {
            System.out.println("hello from thread " + number);
        }
    }

    private static void hello() throws InterruptedException {
        List<Thread> threads = new ArrayList<>();
        for (int i = 1; i <= THREADS; i++) threads.add(new HelloThread(i));
        startAll(threads);
        joinAll(threads);
        System.out.println("Главный поток завершён");
    }

    private static class StopFlag {
        volatile boolean stopped;
    }

    private static void timers(int task, long seconds) throws InterruptedException {
        StopFlag flag = new StopFlag();
        List<Thread> threads = new ArrayList<>();
        for (int i = 1; i <= THREADS; i++) {
            final int number = i;
            threads.add(new Thread(() -> {
                long start = System.nanoTime();
                try {
                    while (!flag.stopped) {
                        Thread.sleep(1000);
                        if (flag.stopped) break;
                        System.out.printf(Locale.ROOT, "thread %d: %.3f s%n", number,
                                (System.nanoTime() - start) / 1_000_000_000.0);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "timer-" + number));
        }
        startAll(threads);
        if (task == 2 && seconds == 0) {
            joinAll(threads); // По заданию таймеры работают бесконечно.
            return;
        }
        Thread.sleep(seconds * 1000);
        if (task == 3) {
            flag.stopped = true; // Только общая память; sleep не прерывается.
        } else {
            interruptAll(threads);
        }
        joinAll(threads);
        System.out.println("Все таймеры завершены");
    }

    private interface Buffer {
        void put(int value) throws InterruptedException;
        int take() throws InterruptedException;
    }

    // В п. 5 эта структура намеренно используется с гонками данных.
    private static class UnsafeBuffer implements Buffer {
        final int[] data = new int[10];
        int head;
        int tail;
        int count;

        @Override
        public void put(int value) throws InterruptedException {
            while (count == data.length) {
                if (Thread.interrupted()) throw new InterruptedException();
                Thread.yield();
            }
            data[tail] = value;
            tail = (tail + 1) % data.length;
            count++;
        }

        @Override
        public int take() throws InterruptedException {
            while (count == 0) {
                if (Thread.interrupted()) throw new InterruptedException();
                Thread.yield();
            }
            int value = data[head];
            head = (head + 1) % data.length;
            count--;
            return value;
        }
    }

    private static class LockBuffer extends UnsafeBuffer {
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition notEmpty = lock.newCondition();
        private final Condition notFull = lock.newCondition();

        @Override
        public void put(int value) throws InterruptedException {
            lock.lockInterruptibly();
            try {
                while (count == data.length) notFull.await();
                data[tail] = value;
                tail = (tail + 1) % data.length;
                count++;
                notEmpty.signal();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public int take() throws InterruptedException {
            lock.lockInterruptibly();
            try {
                while (count == 0) notEmpty.await();
                int value = data[head];
                head = (head + 1) % data.length;
                count--;
                notFull.signal();
                return value;
            } finally {
                lock.unlock();
            }
        }
    }

    private static class MonitorBuffer extends UnsafeBuffer {
        @Override
        public synchronized void put(int value) throws InterruptedException {
            while (count == data.length) wait();
            data[tail] = value;
            tail = (tail + 1) % data.length;
            count++;
            notifyAll();
        }

        @Override
        public synchronized int take() throws InterruptedException {
            while (count == 0) wait();
            int value = data[head];
            head = (head + 1) % data.length;
            count--;
            notifyAll();
            return value;
        }
    }

    private static void producersConsumers(Buffer buffer, boolean safe)
            throws InterruptedException {
        final int itemsPerProducer = 1000;
        // Каждый потребитель пишет только в собственную строку.
        int[][] received = new int[THREADS][THREADS * itemsPerProducer];
        int[] produced = new int[THREADS];
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            final int id = i;
            threads.add(new Thread(() -> {
                try {
                    for (int j = 0; j < itemsPerProducer; j++) {
                        buffer.put(id * itemsPerProducer + j);
                        produced[id]++;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "producer-" + id));
            threads.add(new Thread(() -> {
                try {
                    for (int j = 0; j < itemsPerProducer; j++) {
                        int value = buffer.take();
                        received[id][value]++;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "consumer-" + id));
        }
        startAll(threads);
        if (!safe) {
            // Ошибочный алгоритм может зависнуть; ограничиваем только демонстрацию.
            long deadline = System.nanoTime() + 2_000_000_000L;
            for (Thread thread : threads) {
                long left = deadline - System.nanoTime();
                if (left > 0) thread.join(Math.max(1, left / 1_000_000));
            }
            interruptAll(threads);
        }
        joinAll(threads);
        int missing = 0;
        int duplicates = 0;
        int consumed = 0;
        int sent = 0;
        for (int value = 0; value < THREADS * itemsPerProducer; value++) {
            int occurrences = 0;
            for (int[] row : received) occurrences += row[value];
            consumed += occurrences;
            if (occurrences == 0) missing++;
            if (occurrences > 1) duplicates += occurrences - 1;
        }
        for (int value : produced) sent += value;
        System.out.printf("Произведено: %d; получено: %d; пропущено: %d; повторов: %d%n",
                sent, consumed, missing, duplicates);
        if (safe && (sent != 10000 || missing != 0 || duplicates != 0)) {
            throw new AssertionError("Нарушен обмен данными");
        }
    }

    private static void pi() throws InterruptedException {
        // Остаток Лейбница <= 4 / (2 * terms + 1) < 5e-10.
        final long terms = 4_000_000_000L;
        final int workers = 5;
        final long pairs = terms / 2;
        double[] sums = new double[workers];
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < workers; i++) {
            final int id = i;
            threads.add(new Thread(() -> {
                double sum = 0;
                double correction = 0;
                for (long k = pairs * id / workers; k < pairs * (id + 1) / workers; k++) {
                    double a = 4.0 * k + 1.0;
                    // 1/(4k+1) - 1/(4k+3), без вычитания близких чисел.
                    double value = 2.0 / (a * (a + 2.0));
                    double y = value - correction;
                    double next = sum + y;
                    correction = (next - sum) - y;
                    sum = next;
                }
                sums[id] = sum;
            }, "pi-" + id));
        }
        startAll(threads);
        joinAll(threads);
        double result = 0;
        for (double sum : sums) result += 4.0 * sum;
        double error = Math.abs(result - Math.PI);
        System.out.printf(Locale.ROOT, "pi = %.12f; error = %.3e; threads = %d; terms = %d%n",
                result, error, workers, terms);
        if (error > 1e-9) throw new AssertionError("Погрешность больше 1e-9");
    }

    private static void philosophers() throws InterruptedException {
        ReentrantLock[] forks = new ReentrantLock[7];
        for (int i = 0; i < forks.length; i++) forks[i] = new ReentrantLock(true);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < forks.length; i++) {
            final int id = i;
            threads.add(new Thread(() -> {
                int left = id;
                int right = (id + 1) % forks.length;
                ReentrantLock first = forks[Math.min(left, right)];
                ReentrantLock second = forks[Math.max(left, right)];
                try {
                    for (int meal = 1; meal <= 5; meal++) {
                        pause();
                        first.lockInterruptibly();
                        try {
                            second.lockInterruptibly();
                            try {
                                System.out.printf("Философ %d ест, приём %d%n", id + 1, meal);
                                pause();
                            } finally {
                                second.unlock();
                            }
                        } finally {
                            first.unlock();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "philosopher-" + id));
        }
        startAll(threads);
        joinAll(threads);
        System.out.println("Все 7 философов поели по 5 раз");
    }

    private static void readersWriters() throws InterruptedException {
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
        int[] data = {0};
        List<Thread> threads = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            final int id = i;
            threads.add(new Thread(() -> {
                try {
                    for (int j = 0; j < 5; j++) {
                        pause();
                        lock.writeLock().lockInterruptibly();
                        try {
                            data[0]++;
                            System.out.printf("Писатель %d записал %d%n", id, data[0]);
                            pause();
                        } finally {
                            lock.writeLock().unlock();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "writer-" + id));
        }
        for (int i = 1; i <= 10; i++) {
            final int id = i;
            threads.add(new Thread(() -> {
                try {
                    for (int j = 0; j < 5; j++) {
                        pause();
                        lock.readLock().lockInterruptibly();
                        try {
                            System.out.printf("Читатель %d прочитал %d%n", id, data[0]);
                            pause();
                        } finally {
                            lock.readLock().unlock();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "reader-" + id));
        }
        startAll(threads);
        joinAll(threads);
        System.out.println("Итоговое значение: " + data[0]);
        if (data[0] != 25) throw new AssertionError("Потеряна запись");
    }

    private static void pause() throws InterruptedException {
        Thread.sleep(ThreadLocalRandom.current().nextInt(10, 40));
    }

    private static void shower(long seconds) throws InterruptedException {
        ShowerRoom room = new ShowerRoom();
        List<Thread> threads = new ArrayList<>();
        for (int i = 1; i <= 40; i++) {
            final boolean woman = i <= 20;
            final int id = woman ? i : i - 20;
            threads.add(new Thread(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        pause(); // Свои дела.
                        if (woman) room.womanWantsToEnter();
                        else room.manWantsToEnter();
                        try {
                            Thread.sleep(50); // Мытьё вне блокировки комнаты.
                        } finally {
                            if (woman) room.womanLeaves();
                            else room.manLeaves();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, (woman ? "woman-" : "man-") + id));
        }
        startAll(threads);
        if (seconds > 0) {
            Thread.sleep(seconds * 1000);
            interruptAll(threads);
        }
        joinAll(threads);
        System.out.println("Все 40 посетителей завершили работу; душевая свободна");
    }
}
