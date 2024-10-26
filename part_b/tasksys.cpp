#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */
Tasks::Tasks() {
    finished_tasks_ = -1;
    next_task = -1;
    num_total_tasks_ = -1;
    runnable_ = nullptr;
    status_=1;
}

Tasks::~Tasks() {
}

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    // mutex_ = new std::mutex();
    // finishedMutex_ = new std::mutex();
    // finished_ = new std::condition_variable();
    #ifdef DEBUG
    printf("\n\nCalling constructor \n");
    #endif
    killed_ = false;
    num_runs_started=0;
    num_runs_finished=0;
    num_threads_ = num_threads;
    thread_pool_ = new std::thread[num_threads];
    for (int i = 0; i < num_threads; i++) {
        thread_pool_[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::sleepingThread, this, i);
    }

}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    #ifdef DEBUG
    printf("\n\nDestructor called - %d \n", num_threads_);
    #endif
    // printf("\n\n ================================ Destructor called - %d ================================ \n\n\n", num_threads_);
    // mutex_->lock();
    // killed_ = true;
    // mutex_->unlock();

    //wake up threads to see that killed_ is true
    // finishedMutex_->lock();
    // finishedMutex_->unlock();
    // finished_->notify_all();
    // std::unique_lock<std::mutex> lk(lk_);
    // lk.lock();
    killed_ = true;
    workers.notify_all();
    // lk.unlock();
    for (int i = 0; i < num_threads_; i++){
        if (thread_pool_[i].joinable()) {
            // printf("threads %d have been joined \n", i);
            thread_pool_[i].join();
        }
    }
    // printf("threads have been joined");
    delete[] thread_pool_;
    delete tasks;
    // delete mutex_;
    // delete finishedMutex_;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //
    //increment total active runs 
    // printf("-> Call No : %d \n", num_runs_started);
    int taskID=num_runs_started;
    num_runs_started++;
    
    tasks = new Tasks();

    std::lock_guard<std::mutex> lk(lk_);
    tasks->finished_tasks_ = 0;
    tasks->next_task = 0;
    tasks->num_total_tasks_ = num_total_tasks;
    tasks->runnable_ = runnable;
    tasks->status_ = 0;
    tasks->deps = deps;
    bulk_task_launches[taskID] = tasks;
    #ifdef DEBUG
    for (auto& pair: bulk_task_launches){
        printf("taskID %d", taskID);
        for (auto& dep_:pair.second->deps){
            printf(" | dep %d", dep_);
        }
        printf("\n");
    }
    #endif
    wakeup_signal = true;
    workers.notify_all();
    
    return taskID;
}
void TaskSystemParallelThreadPoolSleeping::sleepingThread(int threadID) {
    int key;
    volatile int taskID;
    volatile int total;
    volatile bool work_not_found = true;
    bool dependencynotMet = false;
    Tasks* workfound;

    #ifdef DEBUG
    printf("threadID %d entering\n", threadID);
    #endif
    
    while (!killed_)
    {   
        // printf("threadID %d looping for\n", threadID);
        std::unique_lock<std::mutex> lk(lk_);
        for (auto& pair: bulk_task_launches){

            // printf("threadID %d looping\n", threadID);
            if (pair.second->status_==1) {
                continue;
            }
            
            dependencynotMet = false;
            //check for any dependencies on the task to exec
            for (auto& depend_:pair.second->deps){
                // Use std::find to check if depend_ is in bulk_task_launches
                auto it = bulk_task_launches.find(depend_);
                if (it != bulk_task_launches.end()) {
                    if (! bulk_task_launches[depend_]->status_){ //status 0 means not ready,not finished,  1 is ready/finished
                        dependencynotMet = true;
                        break;
                    }
                }
            }

            if (dependencynotMet) {
                continue;
            } else {
                work_not_found = false;
                workfound = pair.second;
                key = pair.first;
                break;
            }
            work_not_found = true;
        }
            
        if (!work_not_found) {
            total = workfound->num_total_tasks_;
            taskID = workfound->next_task;
            if (taskID < total) workfound->next_task++;
            try {
                    lk.unlock();
                }
                catch (const std::system_error& e) {
                    std::cerr << "Thread ID : " << threadID << " Caught system_error 3 : " << e.what() << " (code: " << e.code() << ")\n";
                    std::exit(EXIT_FAILURE);
                } catch (const std::exception& e) {
                    std::cerr << "Thread ID : " << threadID << " Caught exception: " << e.what() << std::endl;
                    std::exit(EXIT_FAILURE);
                }

            if (taskID < total) {
                workfound->runnable_->runTask(taskID, total);

                try {
                    lk.lock();
                }
                catch (const std::system_error& e) {
                    std::cerr << "Caught system_error 4 : " << e.what() << " (code: " << e.code() << ")\n";
                    std::exit(EXIT_FAILURE);
                } catch (const std::exception& e) {
                    std::cerr << "Caught exception: " << e.what() << std::endl;
                    std::exit(EXIT_FAILURE);
                }
                workfound->finished_tasks_++;
                // printf("num tasks finished: %d | num runs finished: %d | threadID %d\n", workfound->finished_tasks_, num_runs_finished, threadID);
                if (workfound->finished_tasks_ == total) {
                    num_runs_finished++;
                    // printf("num runs finished: %d threadID %d\n\n", num_runs_finished, threadID);
                    #ifdef DEBUG
                    printf("num runs finished: %d threadID %d\n", num_runs_finished, threadID);
                    #endif

                    workfound->status_=1; //set status of finished task to 1 meaning ready
                    bulk_task_launches.erase(key);
                    wakeup_signal = true;
                    workers.notify_all();

                }

                try {
                    lk.unlock();
                }
                catch (const std::system_error& e) {
                    std::cerr << "Thread ID : " << threadID << " Caught system_error 5 : " << e.what() << " (code: " << e.code() << ")\n";
                    std::exit(EXIT_FAILURE);
                } catch (const std::exception& e) {
                    std::cerr << "Thread ID : " << threadID << " Caught exception: " << e.what() << std::endl;
                    std::exit(EXIT_FAILURE);
                }

            }
        } else {
            wakeup_signal = false;
            // printf("threadID %d -- Sleeping \n", threadID);
            workers.wait(lk, [&]() {
                return killed_ || wakeup_signal;
            });
            // printf("threadID %d -- Wakeup \n", threadID);
        }
        if(num_runs_finished == num_runs_started) {
            finished_.notify_all();
        }
    }

    #ifdef DEBUG
    printf("threadID %d exiting\n", threadID);
    #endif
}

    

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    #ifdef DEBUG
    printf("-> Starting Sync - %d | %d \n", num_runs_finished, num_runs_started);
    #endif
    std::unique_lock<std::mutex> lk(lk_);
    // while(num_runs_finished < num_runs_started);
    if (num_runs_finished < num_runs_started) {
        finished_.wait(lk, [&]() {
            return num_runs_finished >= num_runs_started;
        });
    }
    // killed_ = true;
    #ifdef DEBUG
    printf("-> Ending Sync - %d | %d \n", num_runs_finished, num_runs_started);
    #endif
    // printf("Exiting sync %d \n", num_runs_finished);
    // printf("Exiting sync \n");
    return;
}
