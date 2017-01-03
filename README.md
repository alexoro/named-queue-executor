## NamedQueueExecutor
Android library that implements basic queue logic over one executor.
Currently, if you want to have several background queues you have to create ExecutorService for every queue.
This library extends ThreadPoolExecutor and implements such multi-queue behaviour over single ThreadPoolExecutor.


## How to include in project
Library is distributed via jitpack.io

```gradle
// Add this lines into your roou build.gradle
allprojects {
    repositories {
        jcenter()
        maven { url "https://jitpack.io" }
    }
}
```

```gradle
// Add dependency to library in any target project module
dependencies {
    compile 'com.github.alexoro:named-queue-executor:VERSION'
}
```


## Usage
```java
NamedQueueExecutor namedQueueExecutor = new NamedQueueExecutor();

Callable<Void> task = new Callable<Void>() {
    @Override
    public Void call() throws Exception {
        Thread.sleep(1000);
        return null;
    }
};

Future<?> future = mNamedQueueExecutor.submitNamedQueueTask(
        "CustomQueue",
        task);
```
