package ps.mapreduce.impl;

import ps.mapreduce.impl.jobs.CombineJob;

import java.util.List;

public interface ReduceJob<MK, RV> extends UserJob, CombineJob<MK, RV> {

}
