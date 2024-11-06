package ru.job4j.pooh;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;


public class TopicSchema implements Schema {
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Receiver>> receivers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<String>> messages = new ConcurrentHashMap<>();

    @Override
    public void addReceiver(Receiver receiver) {
        receivers.putIfAbsent(receiver.name(), new CopyOnWriteArrayList<>());
        receivers.get(receiver.name()).add(receiver);
    }

    @Override
    public void publish(Message message) {
        messages.putIfAbsent(message.name(), new CopyOnWriteArrayList<>());
        messages.get(message.name()).add(message.text());
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            for (var topic : messages.keySet()) {
                var messageList = messages.get(topic);
                if (messageList == null || messageList.isEmpty()) {
                    break;
                }
                var topicReceivers = receivers.getOrDefault(topic, new CopyOnWriteArrayList<>());
                while (!messageList.isEmpty()) {
                    var message = messageList.remove(0);
                    for (var receiver : topicReceivers) {
                        receiver.receive(message);
                    }
                }
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
