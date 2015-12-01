package freesqlbase;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.GZIPInputStream;



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class FreeSQLBase {
	static PipedOutputStream pos;
	static PipedInputStream pis;
	
	static String domainsql = "";
	static String entitysql = "";
	static String typesql = "";
	static String propertysql = "";
	static String image_imgsrcsql = "";
	static String imgsrc_urisql = "";
	static String entity_typesql="";
	static String property_schemasql="";
	static String property_expsql="";
	static int id = 0;
	static String obj1 = null;
	static String prop = null;
	static String obj2 = null;
	static Connection con = null;

	
	static void parse(String[] line)
	{
		System.out.printf("%s,%s,%s\n",line[0],line[1],line[2]);
	}
	
	
	static Thread readth = new Thread() {
		@SuppressWarnings("null")
		@Override
		public void run() {
			String line;
			BufferedReader reader = new BufferedReader(new InputStreamReader(pis));
			String mid = null;
			String name = null;
			String description = null;
			
			String image=null;
			String imgsrc=null;
			String uri=null;
			int type = -1;
			int rank = 0;
			int cnt1=1,cnt2=1,cnt3=1;
			Boolean is_ei_read = false, is_pi_read=false;;
			Integer entity_id = 0, property_id = 0, type_id=0;
			String obj1_prev=null;
			String entity=null, property = null;
			//String type_mid;
			String type_idstring = null;
			HashMap<String,Integer> tidstring_id=new HashMap<String, Integer>();
			HashMap<String,Integer> pmid_id=new HashMap<String, Integer>();
			Iterator iter=null;
			PreparedStatement pstmt_ei = null, pstmt_ti = null, pstmt_pi = null;
			ResultSet rs_ei = null, rs_ti = null, rs_pi = null;
			try {
				//pstmt_ei=con.prepareStatement("select mid,id from entity",ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				//pstmt_ei.setFetchSize(Integer.MIN_VALUE);  
				//pstmt_ei.setFetchDirection(ResultSet.FETCH_REVERSE);  
				//pstmt_pi=con.prepareStatement("select mid,id from property");
				pstmt_ti=con.prepareStatement("select idstring,id from type");
				//rs_ei=pstmt_ei.executeQuery();
				//rs_pi=pstmt_pi.executeQuery();
				rs_ti=pstmt_ti.executeQuery();
				while (rs_ti.next())
				{
					tidstring_id.put(rs_ti.getString("idstring"), rs_ti.getInt("id"));
				}
				rs_ti.close();
				pstmt_ti.close();
				
				pstmt_pi=con.prepareStatement("select mid,id from property");
				rs_pi=pstmt_pi.executeQuery();
				while (rs_pi.next())
				{
					pmid_id.put(rs_pi.getString("mid"), rs_pi.getInt("id"));
				}
				rs_pi.close();
				pstmt_pi.close();
				//iter=pmid_id.entrySet().iterator();
				
				/*pstmt_ei=con.prepareStatement("select mid,id from entity",ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				pstmt_ei.setFetchSize(Integer.MIN_VALUE);  
				pstmt_ei.setFetchDirection(ResultSet.FETCH_REVERSE);
				rs_ei=pstmt_ei.executeQuery();*/
				/*if (rs_ei.next())
				{
					entity=rs_ei.getString("mid");
					entity_id=rs_ei.getInt("id");
				}
				else
					is_ei_read=true;
				if (rs_pi.next())
				{
					property=rs_pi.getString("mid");
					property_id=rs_pi.getInt("id");
				}
				else
					is_pi_read=true;*/
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			
			while (true)
			{
				try {
					line = reader.readLine();
					if (line.isEmpty())
					{
						try {
							Statement stmt;
							stmt = con.createStatement();
							if (!entity_typesql.equals(""))
							{
								stmt.executeUpdate("INSERT INTO entity_type VALUES "+entity_typesql.substring(0,entity_typesql.length()-1)+";");
							}
								
							if (!property_schemasql.equals(""))
							{
								stmt.executeUpdate("INSERT INTO property_schema VALUES "+property_schemasql.substring(0,property_schemasql.length()-1)+";");
							}
								
							if (!property_expsql.equals(""))
							{
								stmt.executeUpdate("INSERT INTO property_expectedtype VALUES "+property_expsql.substring(0,property_expsql.length()-1)+";");
							}
							stmt.close();	
						} catch (Exception e) {
							System.out.println("MYSQL ERROR:" + e.getMessage());
						}
						break;
					}
					
					if (cnt1%10001==0)
					{
						System.out.println(entity_id/88767079.0f);
						try {
							Statement stmt;
							stmt = con.createStatement();
							stmt.executeUpdate("INSERT INTO entity_type VALUES "+entity_typesql.substring(0,entity_typesql.length()-1)+";");
							stmt.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
						cnt1=1;
						entity_typesql="";
					}
					
					if (cnt2%10001==0)
					{
						try {
							Statement stmt;
							stmt = con.createStatement();
							stmt.executeUpdate("INSERT INTO property_schema VALUES "+property_schemasql.substring(0,property_schemasql.length()-1)+";");
							stmt.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
						cnt2=1;
						property_schemasql="";
					}
					
					if (cnt3%10001==0)
					{
						try {
							Statement stmt;
							stmt = con.createStatement();
							stmt.executeUpdate("INSERT INTO property_expectedtype VALUES "+property_expsql.substring(0,property_expsql.length()-1)+";");
							stmt.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
						cnt3=1;
						property_expsql="";
					}
					
					
					String[] sp =line.split("\t");
					obj1 = sp[0].substring(1, sp[0].length()-1);
					prop = sp[1].substring(1, sp[1].length()-1);
					obj2 = sp[2];
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
				String[] nspart = obj1.split("ns/");
				if (!nspart[1].startsWith("m."))
					continue;
				if (!obj1.equals(obj1_prev))
				{
					obj1_prev=obj1;
					type = 0;
					entity_id = null;
					property_id = null;
					type_id = null;
					mid=nspart[1];
					mid=mid.substring(2,mid.length());
					if (mid.length()>98)
						mid=mid.substring(0,98);
					try {
						pstmt_ei=con.prepareStatement("select id from entity where mid=?");
						pstmt_ei.setString(1, mid);
						rs_ei=pstmt_ei.executeQuery();
						if (rs_ei.next())
							entity_id=rs_ei.getInt("id");
						rs_ei.close();
						pstmt_ei.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
					property_id=pmid_id.get(mid);
					if (entity_id!=null)
					{
						type=1;
					}
					else if (property_id!=null)
						type=2;
					else type=0;
				}
				if (type==0)
					continue;
				else if (type==1)
				{
					if (prop.indexOf("type.object.type")!=-1)
					{
						String[] nspart2 = obj2.split("ns/");
						type_idstring=nspart2[1];
						type_idstring=type_idstring.substring(0,type_idstring.length()-1);
						type_id=tidstring_id.get(type_idstring);
						//System.out.println(type_idstring+" "+type_id);
						entity_typesql+=String.format("(%d,%d),", entity_id, type_id);	
						cnt1++;
					}
				}
				else if (type==2)
				{
					if (prop.indexOf("type.property.schema")!=-1)
					{
						String[] nspart2 = obj2.split("ns/");
						type_idstring=nspart2[1];
						type_idstring=type_idstring.substring(0,type_idstring.length()-1);
						type_id=tidstring_id.get(type_idstring);
						property_schemasql+=String.format("(%d,%d),", property_id, type_id);	
						cnt2++;
					}
					else if (prop.indexOf("type.property.expected_type")!=-1)
					{
						String[] nspart2 = obj2.split("ns/");
						type_idstring=nspart2[1];
						type_idstring=type_idstring.substring(0,type_idstring.length()-1);
						type_id=tidstring_id.get(type_idstring);
						property_expsql+=String.format("(%d,%d),", property_id, type_id);	
						cnt3++;
					}
				}
			}
		}
	};

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			try {
				//Connection con = null; // 定义一个MYSQL链接对象
				Class.forName("com.mysql.jdbc.Driver").newInstance(); // MYSQL驱动
				//con = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/Freebase", "root","thisismysql"); 
				con = DriverManager.getConnection("jdbc:mysql://202.120.37.25:23334/Freebase", "root","thisismysql"); // 链接本地MYSQL
				} catch (Exception e) {
					System.out.println("MYSQL ERROR:" + e.getMessage());
				}

			pos = new PipedOutputStream(); pis = new PipedInputStream(pos);
			FileInputStream s = new FileInputStream(
					//new File("/home/freebase.gz")); 
					new File("H:/freebase.gz")); 
			readth.start();
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