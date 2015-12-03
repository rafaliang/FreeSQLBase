package freesqlbase;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
//124519884
//123893488
public class FreeSQLBase {
	static PipedOutputStream pos;
	static PipedInputStream pis;
	static Connection con = null; // 定义一个MYSQL链接对象
	static ExecutorService pool = Executors.newFixedThreadPool(2);
	static ExecutorService linepool = Executors.newFixedThreadPool(16);
	//static ThreadPoolExecutor pool = new ThreadPoolExecutor(4, 8, 3, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(200),
    //        new ThreadPoolExecutor.AbortPolicy());
	static SQLBuffer sqlbuf=new SQLBuffer();
	static SQLIdCache sqlcache=new SQLIdCache();
	static HashMap<String,Integer> pidstring_id=new HashMap<String, Integer>();
	static int [] map=new int[8];
	static HashMap<String,Integer> strmap=new HashMap<String, Integer>();
	
	
	static String TrimURL(String url)
	{
		if(url==null)
			return null;
		if(!url.startsWith("<http"))
			return url;
		int last;
		if(url.charAt(url.length()-1)=='>')
		{
			last=url.length()-1;
		}
		else
		{
			last=url.length();
		}
		return url.substring(url.lastIndexOf("/m.")+3,last);
	}
	
	static int getID(String mid)
	{
		int id=0;
		int len=mid.length();
		for (int i=0;i<len;++i)
		{
			id+=map[i];
		}
		return id;
	}
	
	static class KeyNotFoundException extends Exception
	{

		public KeyNotFoundException(String s) {
			super("SQL URL Not found: "+s);
			//System.out.println(s);
			// TODO Auto-generated constructor stub
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = 433079364893439203L;
		
	}
	
	static class SQLIdCache
	{
		final int SIZE=1024*1024;
		int[] cache=new int[SIZE];
		Object[] sync=new Object[SIZE];
		String[] strs=new String[SIZE];
		long hit=0;
		long cnt=1;
		SQLIdCache()
		{
			for(int i=0;i<SIZE;i++)
			{
				sync[i]=new Object();
				cache[i]=-1;
			}
		}
		
		int get(String s) throws KeyNotFoundException {
			// try
			// {
			int i = s.hashCode();
			if (i < 0)
				i = -i;
			i = i % SIZE;
			cnt++;
			synchronized (sync[i])
			{
				if (cache[i] != -1 && strs[i].equals(s)) {
					hit++;
					return cache[i];
				} else {
					int ret = -1;
					PreparedStatement stmt = null;
					try {
						stmt = con.prepareStatement("select id from entity where mid=?");
						stmt.setString(1, s);
						ResultSet res = stmt.executeQuery();
						if (res.next()) {
							ret = res.getInt(1);
						}
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} finally {
						if (stmt != null) {
							try {
								stmt.close();
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
					if (ret == -1)
						throw new KeyNotFoundException(s);
					strs[i]=s;
					cache[i] = ret;
					return ret;

				}
			}
			/*
			 * } catch (Exception e) { //e.printStackTrace(); throw new
			 * KeyNotFoundException(s); }
			 */
		}

	}
	static class SQLTask
	{		
		int id1,id2,prop;
		public SQLTask(int i1,int p,int i2)
		{
			id1=i1;
			id2=i2;
			prop=p;
		}
	}
	
	public static final class StringTask implements Callable<String> {
		PreparedStatement stmt;
		static AtomicInteger cnt=new AtomicInteger(0);
		static AtomicInteger pending_cnt=new AtomicInteger(0);
		static AtomicInteger interval_cnt=new AtomicInteger(0);
		
		public StringTask(PreparedStatement s)
		{
			pending_cnt.incrementAndGet();
			interval_cnt.incrementAndGet();
			stmt=s;
		}
		public String call() {
			
			// Long operations
			try {
				if(stmt==null)
					return null;
				stmt.executeUpdate();
				cnt.incrementAndGet();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		
			finally{
				try {
					if(stmt!=null)
						stmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				pending_cnt.decrementAndGet();
			}
			return null;
		}
	}
	
	static Thread statth = new Thread() {
		@Override
		public void run() {	
			for(;;)
			{
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ThreadDeath e)
				{
					
				}
			
				System.out.printf("[stats] i = %d per = %f, tasksdone = %d, sql_queued = %d, line_queued = %d, hit_rate=%f, sql_tasks=%d\n",
						readth.i,1.0*readth.i/3130753067l,StringTask.cnt.get(),StringTask.pending_cnt.get(),EntryTask.pending.get(),
						1.0*sqlcache.hit/sqlcache.cnt,StringTask.interval_cnt.get());
				sqlcache.hit=0;
				sqlcache.cnt=1;
				StringTask.interval_cnt.set(0);
				
			}
		}
	};
	
	
	static private final class EntryTask implements Callable<String> {
		String[] line;
		int id;
		static AtomicInteger pending=new AtomicInteger(0);
		public EntryTask(String[] lne,int i){
			line=lne;
			id=i;
			pending.incrementAndGet();
		}
		@Override
		public String call() {
			int id1,id2;
			//int id_from_2=-1;
			String p=null;
			int prop=-1;
			try {
					if (line[0].startsWith("<http://rdf.freebase.com/ns/m."))
					{
						id1 = sqlcache.get(TrimURL(line[2]));
						p=line[1].substring(28,line[1].length()-1);
						if (pidstring_id.containsKey(p))
						{
							prop=pidstring_id.get(p);
							if (line[2].startsWith("<http://rdf.freebase.com/ns/m.")){
								id2 = sqlcache.get(TrimURL(line[2]));
								//if (id <= id_from_2)
								sqlbuf.put_et(new SQLTask(id1,prop,id2));
							}
						}
					}
			} catch (KeyNotFoundException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				//System.out.println(e.getMessage());
			}
			finally
			{
				pending.decrementAndGet();
			}
			if(pending.get()<=0 && sqlbuf.readdone)
			{
				System.out.println("Flush buffer");
				sqlbuf.flush();
			}
			return null;
		}
		
	}
	

	static class Entity
	{
		int id;
		int img=-1;
		String des;
		String url;
		Entity(int i,String u)
		{
			url=u;
			id=i;
		}
		void parse (String[] line)
		{
			
		}
		void end()
		{

		}
	}

	
	static class ReaderThread extends  Thread {

		public long i = 0;
		public int id=0;

		

		@Override
		public void run() {
			BufferedReader reader = new BufferedReader(new InputStreamReader(pis));
			String last=null;
			int id=-1;
			int rank=0;
			Entity cur=null;
			for (;;) {
				i++;
				
				String line = null;
				try {
					line = reader.readLine();
					if(line.isEmpty())
					{
						System.out.printf("Almost done\n");
						//sqlbuf.put_rank(new SQLTask(id,rank));
						sqlbuf.done();
						if(EntryTask.pending.get()==0)
						{
							System.out.println("Main thread Flush");
							sqlbuf.flush();
						}
						//statth.stop();
						break;
					}
					
					while(EntryTask.pending.get()>500000 
							|| StringTask.pending_cnt.get()>300)
					{
						//System.out.println("Queue too long, sleeping...");
						try {
							Thread.sleep(500);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					String[] sp=line.split("\t");
					///////////////////////////////////////////////
					if(!sp[0].equals(last))
					{
						
						if(id!=-1)
						{
							//cur.end();
							//this line should be in use when running the full adaption
							//sqlbuf.put_rank(new SQLTask(id,rank));
						}
						rank=0;
						last=sp[0];
						if(!last.startsWith("<http://rdf.freebase.com/ns/m."))
							continue;
						id++;
						/*try
						{
							id=sqlcache.get(TrimURL(last));
						}catch (KeyNotFoundException e) {
							// TODO Auto-generated catch block
							//e.printStackTrace();
							//System.out.println(e.getMessage());
						}*/
							
						//cur=new Entity(id,sp[0]);
					}
					rank++;
					//cur.parse(sp);
					//if(id<=124519884)
					//	continue;
					///////////////////////////////////////////////		
					
					
					//this line should be in use when running the full adaption
					linepool.submit(new EntryTask(sp,id));				
				}
				catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			try {
				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	};
	static ReaderThread readth = new ReaderThread();
	
	public static void main(String[] args) {
		map[0]=0;
		for (int i=1;i<8;++i)
		{
			map[i]=(int) Math.pow(32, 7-i);
		}
		strmap.put("0", 0);
		strmap.put("1", 1);
		strmap.put("2", 2);
		strmap.put("3", 3);
		strmap.put("4", 4);
		strmap.put("5", 5);
		strmap.put("6", 6);
		strmap.put("7", 7);
		strmap.put("8", 8);
		strmap.put("9", 9);
		strmap.put("b", 0);
		strmap.put("c", 1);
		strmap.put("d", 2);
		strmap.put("f", 3);
		strmap.put("g", 4);
		strmap.put("h", 5);
		strmap.put("j", 6);
		strmap.put("k", 7);
		strmap.put("l", 8);
		strmap.put("m", 9);
		strmap.put("n", 0);
		strmap.put("p", 1);
		strmap.put("d", 2);
		strmap.put("f", 3);
		strmap.put("g", 4);
		strmap.put("h", 5);
		strmap.put("j", 6);
		strmap.put("k", 7);
		strmap.put("l", 8);
		strmap.put("m", 9);
		// TODO Auto-generated method stub
		try {
			try {
				//create table main( url varchar(256), name varchar(60000), type varchar(128), id int(32) primary key, rank int(32));
				//create table other(e_id int(32), t_id int(32), primary key(e_id,t_id));
				//2*Maxint-1164214229=3130753067
				
				Class.forName("com.mysql.jdbc.Driver").newInstance(); // MYSQL驱动
				//con = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/Freebase", "root", "thisismysql"); // 链接本地MYSQL
				con = DriverManager.getConnection("jdbc:mysql://202.120.37.25:23334/Freebase", "root","thisismysql"); // 链接本地MYSQL
				System.out.println("Connected to MYSQL");
				
				try {
					PreparedStatement pstmt_pi=con.prepareStatement("select idstring,id from property");
					ResultSet rs_pi=pstmt_pi.executeQuery();
					while (rs_pi.next())
					{
						pidstring_id.put(rs_pi.getString("idstring"), rs_pi.getInt("id"));
					}
					rs_pi.close();
					pstmt_pi.close();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}	
				
				/*Statement stmt;
				stmt = con.createStatement();
				stmt.executeUpdate("INSERT INTO test VALUES (1,'KK')");
				ResultSet res = stmt.executeQuery("select * from test");
				int ret_id;
				String name;
				if (res.next()) {
					ret_id = res.getInt(1);
					name = res.getString(2);
					System.out.println(ret_id+" "+name);
				}*/
			} catch (Exception e) {
				System.out.println("MYSQL ERROR:" + e.getMessage());
			} 

			pos = new PipedOutputStream(); pis = new PipedInputStream(pos);
			FileInputStream s = new FileInputStream(
					//new File("/home/freebase.gz")); 
					new File("H:/freebase.gz")); 
			readth.start();
			statth.start();
			decompress(s);
			 
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void decompress(InputStream is) throws Exception {

		GZIPInputStream gis = new GZIPInputStream(is);
		int count;
		byte data[] = new byte[4096];

		while ((count = gis.read(data, 0, 4096)) != -1) {
			pos.write(data, 0, count);
		}
		pos.write("\n".getBytes());
		gis.close();
	}
}