package setup;

import job.*;

public class SetupJob {

    @SuppressWarnings("static-access")
	public static void main(String[] args) throws Exception {
    	job1 j1 = new job1();
        job2 j2 = new job2();
        job3 j3 = new job3();
        job4 j4 = new job4();
        job5 j5 = new job5();
        job6 j6 = new job6();
        job7 j7 = new job7();
        
        j2.main(args);
        j4.main(args);
        j1.main(args);
        j3.main(args);
        j5.main(args);
        j6.main(args);
        j7.main(args);
        
        
    }
    
}