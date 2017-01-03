package com.alexoro.concurrent.sample;

import android.app.Activity;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;

import com.alexoro.concurrent.NamedQueueExecutor;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by uas.sorokin@gmail.com
 */
public class MainActivity extends Activity {

    private static final String NAMED_QUEUE_EXECUTOR_NAME_MASK = "NamedQueue:Task-%d";

    private NamedQueueExecutor mNamedQueueExecutor;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.main);

        mNamedQueueExecutor = createNamedQueueExecutorService();

        Button buttonLaunch = (Button) findViewById(R.id.launch);
        buttonLaunch.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                launch();
            }
        });
    }

    private void launch() {
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
    }

    private static NamedQueueExecutor createNamedQueueExecutorService() {
        return new NamedQueueExecutor(
                0, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>());
    }

}