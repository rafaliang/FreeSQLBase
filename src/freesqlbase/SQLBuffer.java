package freesqlbase;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import freesqlbase.FreeSQLBase.SQLTask;
//import freesqlbase.FreeSQLBase.SQLTask2;
import freesqlbase.FreeSQLBase.StringTask;

public class SQLBuffer{
	final int TASKS=16*1024;
	SQLTask[] tsk_et=new SQLTask[TASKS];
	
	int tsk_cnt_et=0;

	boolean readdone=false;


	
	PreparedStatement getStmt(SQLTask[] task,int count)
	{
		PreparedStatement stmt=null;
		try {
			StringBuffer buf=new StringBuffer();
			buf.append("INSERT INTO test Values (?,?,?)");
			for(int i=1;i<count;i++)
			{
				buf.append(",(?,?,?)");
			}
			stmt = FreeSQLBase.con.prepareStatement(buf.toString());
		
			for(int i=0;i<count;i++)
			{
				stmt.setInt(i*3+1, task[i].id1);
				stmt.setInt(i*3+2, task[i].prop);
				stmt.setInt(i*3+3, task[i].id2);
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return stmt;
	}
	
	
	
	void put_et(SQLTask t)
	{
		synchronized(tsk_et)
		{
			tsk_et[tsk_cnt_et]=t;
			tsk_cnt_et++;
			if(tsk_cnt_et==TASKS)
			{
				FreeSQLBase.pool.submit(new StringTask(getStmt(tsk_et,tsk_cnt_et)));
				tsk_cnt_et=0;
				tsk_et=new SQLTask[TASKS];
			}
		}
	}
	
	void done()
	{
		readdone=true;
	}
	
	void flush()
	{
		System.out.printf("flush tsk_cnt_te=%d\n"
				,tsk_cnt_et);
		
		synchronized(tsk_et)
		{
			if(tsk_cnt_et!=0)
			{
				FreeSQLBase.pool.submit(new StringTask(getStmt(tsk_et,tsk_cnt_et)));
				tsk_cnt_et=0;
				tsk_et=null;
			}
		}
		
	}
}