import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;


public class NoLiveLocks {
    public static int num_taks=30;
    public static int num_queries_per_task = 3; // to provoke starvation: increase num_taks, num_queries_per_task or threads

    Connection con;
    Random rand = new Random(123456);
    int num_tuple = 50000;
    double max_value = 50.0f;
    AtomicInteger tasks_finished;

    public NoLiveLocks(){
        tasks_finished = new AtomicInteger(0);
    }

    void executeSQL(String sql, Connection con){
        java.sql.Statement stmt;
        try {
            stmt = con.createStatement();
            stmt.execute(sql);
            System.out.println("Executing "+sql);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void createDB(){
        try {
            Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
            executeSQL("DROP TABLE IF EXISTS blocking_data;",con);
            executeSQL("CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer);",con);

            PreparedStatement ps;
            con.setAutoCommit(false);
            ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)",ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

            for(int i=0;i<num_tuple;i++){
                ps.setInt(1, i);
                ps.setDouble(2,rand.nextDouble()*max_value);
                ps.setInt(3, 0);
                ps.addBatch();
                ps.clearParameters();
            }

            int[] results = ps.executeBatch();
            con.commit();
            con.close();
            System.out.println(Arrays.toString(results));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        //Create DB
        NoLiveLocks b = new NoLiveLocks();
        b.createDB();

        //Create task and their queries
        ArrayList<NoLiveLocks.Blocker> my_tasks = new ArrayList<NoLiveLocks.Blocker>(num_taks);
        for(int task=0;task<num_taks;task++){
            my_tasks.add(b.new Blocker(task));
        }

        //Execute tasks
        ThreadPoolExecutor executor =
                (ThreadPoolExecutor) Executors.newFixedThreadPool(4);//XXX - Do not change the number of threads used
        double start, stop;
        start = System.currentTimeMillis();
        for(NoLiveLocks.Blocker task : my_tasks) {
            executor.execute(task);
        }
        executor.shutdown();

        //wait for the tasks to finish
        while(!executor.isTerminated()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        stop = System.currentTimeMillis();
        System.out.println("[DONE] Task execution after roughly "+(stop-start)+" ms finished: "+b.tasks_finished+" of "+num_taks);
    }

    /**
     * This is simply a task attempting to hold many write locks for a long time and updating major parts of the database.
     *
     */
    public class Blocker implements Runnable  {
        final int task;
        final double [] start_range;
        final double [] stop_range;
        // Counts how often this task was rolled back as deadlock victim.
        int rollbacks = 0;

        public Blocker(int id){
            this.task = id;
            this.start_range = new double[num_queries_per_task];
            this.stop_range  = new double[num_queries_per_task];
            for(int q=0;q<num_queries_per_task;q++) {
                stop_range[q]  = rand.nextDouble()*max_value;
                start_range[q] = rand.nextDouble()*stop_range[q];
            }
        }

        @Override
        public void run() {
            Connection con=null;//XXX - Yes there needs to be a separate Connection per blocking taks. Otherwise strange things can happen
            con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);//XXX
            PreparedStatement ps;

            try {
                // pg_stat_activity
                con.createStatement().execute("SET application_name = 'Task " + task + " (" + rollbacks + " rollbacks)'");
                con.setAutoCommit(false);
                con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                // Starvation fix task that was the deadlock victim 2 times gets priority.
                // LOCK TABLE takes an exclusive lock on the whole table so no other transaction can lock any row.
                // The task runs alone, cannot be a victim again and commits. The lock is released at commit (VO2 p.11).
                if (rollbacks >= 2) {
                    con.createStatement().execute("LOCK TABLE blocking_data");
                    System.out.println("Task " + task + " has priority: runs alone");
                }
                int q = 0;
                ps = con.prepareStatement("UPDATE blocking_data SET data_value = ?,  modified_by = "+task+" WHERE data_value >= ? AND data_value <= ?;",ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

                while(q<num_queries_per_task) {//I want to execute num_queries_per_task update queries
                    ps.setDouble(1, (float) (rand.nextDouble()*max_value));//some value, which one is not important.
                    ps.setDouble(2, start_range[q]);
                    ps.setDouble(3, stop_range[q]);
                    //System.out.println("Started query "+q+" task "+task+" Executing "+ps);
                    ps.execute();
                    q++;
                }
                con.commit();
                int num_finished = tasks_finished.incrementAndGet();
                con.close();
                // Shows how many rollbacks this task needed. With the fix no task goes above 2.
                System.out.println("Done task "+task+" after "+rollbacks+" rollbacks. I am finisher number "+num_finished);
            } catch (SQLException e) {
                System.err.println("Task "+task+" "+e.getMessage());
                // increase rollback
                rollbacks++;

                // rollback() ends the aborted transaction, close() frees the connection,
                // run() starts the whole transaction again with a new connection.

                try {
                    con.rollback();
                    con.close();
                }

                catch (SQLException e2) {

                }

                run();

            }
        }
    }
}
