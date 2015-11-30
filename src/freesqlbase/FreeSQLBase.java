package freesqlbase;

import java.io.*;
import java.nio.ByteBuffer;
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
	static int id = 0;
	static String obj1 = null;
	static String prop = null;
	static String obj2 = null;
	static Connection con = null;
	static PreparedStatement pstmt1 = null,pstmt2 = null,pstmt3=null,pstmt4=null,pstmt5=null,pstmt0=null;

	
	static void parse(String[] line)
	{
		System.out.printf("%s,%s,%s\n",line[0],line[1],line[2]);
	}
	
	
	static Thread readth = new Thread() {
		@Override
		public void run() {
			String line;
			BufferedReader reader = new BufferedReader(new InputStreamReader(pis));
			String mid = null;
			String name = null;
			String description = null;
			String idstring = null;
			String image=null;
			String imgsrc=null;
			String uri=null;
			int type = 0;
			int rank = 0;
			int cnt0=1,cnt1=1,cnt2=1,cnt3=1,cnt4=1,cnt5=1;
			String obj1_prev=null;
			
			
			/*try
			{
				con.setAutoCommit(false);
				pstmt1=con.prepareStatement("INSERT INTO domain VALUES (?,?,?,?)");
				pstmt2=con.prepareStatement("INSERT INTO type VALUES (?,?,?,?)");
				pstmt3=con.prepareStatement("INSERT INTO property VALUES (?,?,?,?)");
				pstmt4=con.prepareStatement("INSERT INTO image_imgsrc VALUES (?,?)");
				pstmt5=con.prepareStatement("INSERT INTO imgsrc_uri VALUES (?,?)");
				pstmt0=con.prepareStatement("INSERT INTO entity VALUES (?,?,?,?,?,?)");
			}
			catch (Exception e) {
				System.out.println("MYSQL ERROR:" + e.getMessage());
			}*/
			
			while (true)
			{
				try {
					line = reader.readLine();
					if (line.isEmpty())
					{
						try {
							Statement stmt;
							stmt = con.createStatement();
							if (!domainsql.equals(""))
							{
								pstmt1.executeBatch();
								con.commit();
								//pstmt1.clearBatch();
								//stmt.executeUpdate("INSERT INTO domain VALUES "+domainsql.substring(0,domainsql.length()-1)+";");
								
							}
								
							if (!typesql.equals(""))
							{
								pstmt2.executeBatch();
								con.commit();
								//pstmt2.clearBatch();
								//stmt.executeUpdate("INSERT INTO type VALUES "+typesql.substring(0,typesql.length()-1)+";");
							}
								
							if (!propertysql.equals(""))
							{
								pstmt3.executeBatch();
								con.commit();
								//pstmt3.clearBatch();
								//stmt.executeUpdate("INSERT INTO property VALUES "+propertysql.substring(0,propertysql.length()-1)+";");
							}
								
							if (!entitysql.equals(""))
							{
								pstmt0.executeBatch();
								con.commit();
								//pstmt0.clearBatch();
								//stmt.executeUpdate("INSERT INTO entity VALUES "+entitysql.substring(0,entitysql.length()-1)+";");
							}
								
							if (!image_imgsrcsql.equals(""))
							{
								pstmt4.executeBatch();
								con.commit();
								//pstmt4.clearBatch();
								//stmt.executeUpdate("INSERT INTO image_imgsrc VALUES "+image_imgsrcsql.substring(0,image_imgsrcsql.length()-1)+";");
							}
								
							if (!imgsrc_urisql.equals(""))
							{
								pstmt5.executeBatch();
								con.commit();
								//pstmt5.clearBatch();
								//stmt.executeUpdate("INSERT INTO imgsrc_uri VALUES "+imgsrc_urisql.substring(0,imgsrc_urisql.length()-1)+";");
							}
							//stmt.close();	
						} catch (Exception e) {
							System.out.println("MYSQL ERROR:" + e.getMessage());
						}
						break;
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
					if (obj1_prev!=null)
					{
						if (type==1)
						{
							try {
								pstmt1.setInt(1,id);
								pstmt1.setString(2, mid);
								pstmt1.setString(3,name);
								pstmt1.setString(4,idstring);
								pstmt1.addBatch();
							} catch (SQLException e) {
								e.printStackTrace();
							}
							//domainsql+=String.format("(%d,%s,%s,%s),", id, mid, name, idstring);
							cnt1++;
						}
						else if (type==2)
						{
							try {
								pstmt2.setInt(1,id);
								pstmt2.setString(2, mid);
								pstmt2.setString(3,name);
								pstmt2.setString(4,idstring);
								pstmt2.addBatch();
							} catch (SQLException e) {
								e.printStackTrace();
							}
							//id++;
							//typesql+=String.format("(%d,%s,%s,%s),", id, mid, name, idstring);
							cnt2++;
						}
						else if (type==3)
						{
							try {
								pstmt3.setInt(1,id);
								pstmt3.setString(2, mid);
								pstmt3.setString(3,name);
								pstmt3.setString(4,idstring);
								pstmt3.addBatch();
							} catch (SQLException e) {
								e.printStackTrace();
							}
							//id++;
							//propertysql+=String.format("(%d,%s,%s,%s),", id, mid, name, idstring);
							cnt3++;
						}
						else if (type==4)
						{
							//id++;
							//System.out.printf("%s,%s\n",mid, imgsrc);
							try {
								pstmt4.setString(1,mid);
								pstmt4.setString(2, imgsrc);
								pstmt4.addBatch();
							} catch (SQLException e) {
								e.printStackTrace();
							}
							image_imgsrcsql+=String.format("(%s,%s),", mid, imgsrc);
							cnt4++;
						}
						else if (type==5)
						{
							try {
								pstmt5.setString(1,mid);
								pstmt5.setString(2, imgsrc);
								pstmt5.addBatch();
							} catch (SQLException e) {
								e.printStackTrace();
							}
							//id++;
							//imgsrc_urisql+=String.format("(%s,%s),", mid, uri);
							cnt5++;
						}
						else
						{
							try {
								pstmt0.setInt(1,id);
								pstmt0.setString(2, mid);
								pstmt0.setInt(3,rank);
								pstmt0.setString(4, name);
								pstmt0.setString(5,description);
								pstmt0.setString(6, image);
								pstmt0.addBatch();
							} catch (SQLException e) {
								e.printStackTrace();
							}
							//id++;
							//entitysql+=String.format("(%d,%s,%d,%s,%s,%s),", id, mid, rank, name, description, image);
							cnt0++;
						}
						
						if (cnt0%10000==0)
						{
							System.out.println(id/88788059.0f);
							try {
								pstmt0.executeBatch();
								con.commit();
								//pstmt0.clearBatch();
							} catch (Exception e) {
								System.out.println("MYSQL ERROR:" + e.getMessage());
							}
							cnt0=1;
							//entitysql = "";
						}
						
						if (cnt1%10000==0)
						{
							System.out.println(id/88788059.0f);
							try {
								pstmt1.executeBatch();
								con.commit();
								//pstmt1.clearBatch();
							} catch (Exception e) {
								System.out.println("MYSQL ERROR:" + e.getMessage());
							}
							cnt1=1;
							//domainsql = "";
						}
						
						if (cnt2%10000==0)
						{
							System.out.println(id/88788059.0f);
							try {
								pstmt2.executeBatch();
								con.commit();
								//pstmt2.clearBatch();
							} catch (Exception e) {
								System.out.println("MYSQL ERROR:" + e.getMessage());
							}
							cnt2=1;
							//typesql = "";
						}
						
						if (cnt3%10000==0)
						{
							System.out.println(id/88788059.0f);
							try {
								pstmt3.executeBatch();
								con.commit();
								//pstmt3.clearBatch();
							} catch (Exception e) {
								System.out.println("MYSQL ERROR:" + e.getMessage());
							}
							cnt3=1;
							//propertysql = "";
						}
						
						if (cnt4%10000==0)
						{
							System.out.println(id/88788059.0f);
							try {
								pstmt4.executeBatch();
								con.commit();
								//pstmt4.clearBatch();
							} catch (Exception e) {
								System.out.println("MYSQL ERROR:" + e.getMessage());
							}
							cnt4=1;
							//image_imgsrcsql = "";
						}
						
						if (cnt5%10000==0)
						{
							System.out.println(id/88788059.0f);
							try {
								pstmt5.executeBatch();
								con.commit();
								//pstmt5.clearBatch();
							} catch (Exception e) {
								System.out.println("MYSQL ERROR:" + e.getMessage());
							}
							cnt5=1;
							//imgsrc_urisql = "";
						}
						
						
						
						
					}
					obj1_prev=obj1;
					id++;
					rank=0;
					mid = null;
					name = null;
					description = null;
					idstring = null;
					image=null;
					type = 0;
					mid=nspart[1];
					mid=mid.substring(2,mid.length());
					if (mid.length()>98)
						mid=mid.substring(0,98);
					//mid="\""+mid+"\"";
				}
				rank++;
				if (prop.indexOf("type.object.id")!=-1)
				{
					String[] obj2_new = obj2.substring(1, obj2.length()-1).split("ns/");
					idstring = obj2_new[obj2_new.length-1];
					idstring=idstring.substring(1,idstring.length());
					idstring=idstring.replaceAll("/",".");
					if (idstring.length()>98)
						idstring=idstring.substring(0,98);
					//idstring="\""+idstring+"\"";
				}
				else if (prop.indexOf("type.object.type")!=-1)
				{
					if (obj2.indexOf("type.domain")!=-1)
					{
						type = 1;
					}
					if (obj2.indexOf("type.type")!=-1)
					{
						type = 2;
					}
					if (obj2.indexOf("type.property")!=-1)
					{
						type = 3;
					}
					if (obj2.indexOf("common.image")!=-1)
					{
						type = 4;
					}
					if (obj2.indexOf("type.content_import")!=-1)
					{
						type = 5;
					}
				}
				else if (prop.indexOf("type.object.name")!=-1)
				{
					if (obj2.endsWith("@en"))
					{
						name=obj2.substring(1, obj2.length()-4);
						/*if (name!=null)
							name=name.replaceAll("\"", "'");*/
						if (name.length()>29998)
							name=name.substring(0,29998);
						//name = "\""+name+"\"";
					}
				}
				else if (prop.indexOf("common.topic.description")!=-1)
				{
					if (obj2.endsWith("@en"))
					{
						
						description = obj2.substring(1, obj2.length()-4);
						/*if (description!=null)
							description=description.replaceAll("\"", "'");*/
						if (description.length()>1998)
							description=description.substring(0,1998);
							/*description="\""+description.substring(0,1998)+"\"";
						else
							description="\""+description+"\"";*/
					}
				}
				else if (prop.indexOf("common.topic.image")!=-1)
				{
					String[] obj2_new = obj2.substring(1, obj2.length()-1).split("ns/m.");
					image=obj2_new[obj2_new.length-1];
					if (image.length()>98)
						image=image.substring(0,98);
					//image = "\""+image+"\"";
				}
				else if (prop.indexOf("type.content.source")!=-1)
				{
					String[] obj2_new = obj2.substring(1, obj2.length()-1).split("ns/m.");
					imgsrc=obj2_new[obj2_new.length-1];
					if (imgsrc.length()>98)
						imgsrc=imgsrc.substring(0,98);
					//imgsrc = "\""+imgsrc+"\"";
				}
				else if (prop.indexOf("type.content_import.uri")!=-1)
				{
					uri=obj2.substring(1,obj2.length()-1);
					if (uri.length()>498)
						uri=uri.substring(0,498);
					//uri = "\""+uri+"\"";
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
				//con = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/Freebase", "root","thisismysql"); // 链接本地MYSQL		
				con = DriverManager.getConnection("jdbc:mysql://202.120.37.25:23334/Freebase", "root", "thisismysql");
				con.setAutoCommit(false);	
				pstmt1=con.prepareStatement("INSERT INTO domain VALUES (?,?,?,?)");
				pstmt2=con.prepareStatement("INSERT INTO type VALUES (?,?,?,?)");
				pstmt3=con.prepareStatement("INSERT INTO property VALUES (?,?,?,?)");
				pstmt4=con.prepareStatement("INSERT INTO image_imgsrc VALUES (?,?)");
				pstmt5=con.prepareStatement("INSERT INTO imgsrc_uri VALUES (?,?)");
				pstmt0=con.prepareStatement("INSERT INTO entity VALUES (?,?,?,?,?,?)");
				
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