import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ShowerRoom {
    private enum Gender { WOMAN, MAN }

    private final ReentrantLock lock = new ReentrantLock(true);
    private final Condition changed = lock.newCondition();
    // Очередь заявок: новые посетители не обходят ожидающий противоположный пол.
    private final Deque<Object> queue = new ArrayDeque<>();
    private Gender gender;
    private int inside;

    public void womanWantsToEnter() throws InterruptedException {
        enter(Gender.WOMAN);
    }

    public void manWantsToEnter() throws InterruptedException {
        enter(Gender.MAN);
    }

    public void womanLeaves() {
        leave(Gender.WOMAN);
    }

    public void manLeaves() {
        leave(Gender.MAN);
    }

    private void enter(Gender requested) throws InterruptedException {
        lock.lockInterruptibly();
        Object ticket = new Object();
        try {
            queue.addLast(ticket);
            try {
                while (queue.peekFirst() != ticket || inside == 10
                        || (inside > 0 && gender != requested)) {
                    changed.await();
                }
            } catch (InterruptedException e) {
                queue.remove(ticket);
                changed.signalAll();
                throw e;
            }
            queue.removeFirst();
            gender = requested;
            inside++;
            printState("enter");
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void leave(Gender departing) {
        lock.lock();
        try {
            if (inside == 0 || gender != departing) {
                throw new IllegalStateException("Выход из пустой комнаты или неверный пол");
            }
            inside--;
            if (inside == 0) gender = null;
            printState("leave");
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void printState(String action) {
        String state = gender == null ? "EMPTY" : gender.name();
        System.out.printf("%s %s: inside=%d state=%s%n",
                Thread.currentThread().getName(), action, inside, state);
    }
}
