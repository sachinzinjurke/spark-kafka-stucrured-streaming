package com.example.spark.streaming.query;

import org.apache.spark.sql.streaming.StreamingQueryListener;

public class CustomQueryListener extends StreamingQueryListener {
    @Override
    public void onQueryStarted(QueryStartedEvent queryStarted) {
        System.out.println("Query started: " + queryStarted.id());

    }

    @Override
    public void onQueryProgress(QueryProgressEvent queryProgress) {
        System.out.println("Query made progress: " + queryProgress.progress().prettyJson());

    }

    @Override
    public void onQueryTerminated(QueryTerminatedEvent queryTerminated) {
        System.out.println("Query terminated: " + queryTerminated.id());

    }
}
