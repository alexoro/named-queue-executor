/*
 * Copyright (C) 2016 Alexander Sorokin (alexoro)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.alexoro.concurrent;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by uas.sorokin@gmail.com
 */
public class NamedQueueExecutor extends ThreadPoolExecutor {

    private final Object mIntegrityLock = new Object();
    private final Map<String, Queue<Callable<?>>> mNamedQueue = new HashMap<>();
    private final Map<Callable<?>, NamedQueueFuture<?>> mCallableFutureMap = new HashMap<>();


    public NamedQueueExecutor(int corePoolSize,
                              int maximumPoolSize,
                              long keepAliveTime,
                              TimeUnit unit,
                              BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    public NamedQueueExecutor(int corePoolSize,
                              int maximumPoolSize,
                              long keepAliveTime,
                              TimeUnit unit,
                              BlockingQueue<Runnable> workQueue,
                              ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    public NamedQueueExecutor(int corePoolSize,
                              int maximumPoolSize,
                              long keepAliveTime,
                              TimeUnit unit,
                              BlockingQueue<Runnable> workQueue,
                              RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
    }

    public NamedQueueExecutor(int corePoolSize,
                              int maximumPoolSize,
                              long keepAliveTime,
                              TimeUnit unit,
                              BlockingQueue<Runnable> workQueue,
                              ThreadFactory threadFactory,
                              RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }


    public <V> Future<V> submitNamedQueueTask(String queueName, Callable<V> callable) {
        if (queueName == null) {
            throw new IllegalArgumentException("queueName is null");
        }

        synchronized (mIntegrityLock) {
            assertNotShutdown();

            Queue<Callable<?>> queue = mNamedQueue.get(queueName);
            if (queue == null) {
                queue = new LinkedList<>();
                mNamedQueue.put(queueName, queue);
            }
            queue.offer(callable);

            if (queue.size() == 1) {
                Callable<V> wrapCallable = new NamedQueueCallableWrapper<>(queueName, callable);
                Future<V> future = submit(wrapCallable);
                NamedQueueFuture<V> wrapFuture = new NamedQueueFuture<>(
                        queueName,
                        callable);
                wrapFuture.setExecutorFuture(future);
                return wrapFuture;
            } else {
                NamedQueueFuture<V> wrapFuture = new NamedQueueFuture<>(
                        queueName,
                        callable);
                mCallableFutureMap.put(callable, wrapFuture);
                return wrapFuture;
            }
        }
    }

    private void runNextCallableOnNamedQueue(String queueName) {
        synchronized (mIntegrityLock) {
            Queue<Callable<?>> queue = mNamedQueue.get(queueName);
            if (queue != null) {
                Callable<?> callable = queue.peek();
                if (callable != null) {
                    Callable<?> wrapCallable = new NamedQueueCallableWrapper<>(queueName, callable);
                    Future<?> future = submit(wrapCallable);
                    NamedQueueFuture<?> wrapFuture = mCallableFutureMap.remove(callable);
                    wrapFuture.setExecutorFuture(future);
                }
            }
        }
    }

    private void assertNotShutdown() {
        synchronized (mIntegrityLock) {
            if (isShutdown()) {
                throw new RejectedExecutionException("Executor is released");
            }
        }
    }

    private class NamedQueueCallableWrapper<V> implements Callable<V> {

        private final String mQueueName;
        private final Callable<V> mCallable;

        public NamedQueueCallableWrapper(String queueName, Callable<V> callable) {
            mQueueName = queueName;
            mCallable = callable;
        }

        @Override
        public V call() throws Exception {
            try {
                return mCallable.call();
            } finally {
                synchronized (mIntegrityLock) {
                    Queue<?> queue = mNamedQueue.get(mQueueName);
                    if (queue != null) {
                        queue.remove(mCallable);
                        if (queue.isEmpty()) {
                            mNamedQueue.remove(mQueueName);
                        } else {
                            runNextCallableOnNamedQueue(mQueueName);
                        }
                    }
                }
            }
        }

    }

    private class NamedQueueFuture<V> implements Future<V> {

        private final String mQueueName;
        private final Callable<?> mCallable;
        private final Object mExecutorFutureSetEvent;
        private volatile Future<?> mExecutorFuture;
        private volatile Boolean mCancelResult;

        public NamedQueueFuture(String queueName, Callable<?> callable) {
            mQueueName = queueName;
            mCallable = callable;
            mExecutorFutureSetEvent = new Object();
            mExecutorFuture = null;
            mCancelResult = null;
        }

        public void setExecutorFuture(Future<?> executorFuture) {
            synchronized (mIntegrityLock) {
                mExecutorFuture = executorFuture;
                synchronized (mExecutorFutureSetEvent) {
                    mExecutorFutureSetEvent.notify();
                }
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            synchronized (mIntegrityLock) {
                if (mCancelResult == null) {
                    if (mExecutorFuture == null) {
                        Queue<?> queue = mNamedQueue.get(mQueueName);
                        if (queue != null) {
                            queue.remove(mCallable);
                            if (queue.isEmpty()) {
                                mNamedQueue.remove(mQueueName);
                            }
                        }
                        mCallableFutureMap.remove(mCallable);
                        mCancelResult = true;
                    } else {
                        mCancelResult = mExecutorFuture.cancel(mayInterruptIfRunning);
                    }
                }
                return mCancelResult;
            }
        }

        @Override
        public boolean isCancelled() {
            synchronized (mIntegrityLock) {
                return (mCancelResult != null);
            }
        }

        @Override
        public boolean isDone() {
            synchronized (mIntegrityLock) {
                if (mCancelResult != null) {
                    return true;
                } else {
                    if (mExecutorFuture == null) {
                        return false;
                    } else {
                        return mExecutorFuture.isDone();
                    }
                }
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public V get() throws InterruptedException, ExecutionException {
            while (true) {
                final Boolean cancelResult;
                final Future<?> executorFuture;
                synchronized (mIntegrityLock) {
                    cancelResult = mCancelResult;
                    executorFuture = mExecutorFuture;
                }
                synchronized (mExecutorFutureSetEvent) {
                    if (cancelResult != null) {
                        throw new ExecutionException(new CancellationException());
                    } else if (executorFuture == null) {
                        mExecutorFutureSetEvent.wait();
                    } else {
                        return (V) executorFuture.get();
                    }
                }
            }
        }

        @Override
        public V get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            throw new UnsupportedOperationException("Not implemented");
        }

    }

}