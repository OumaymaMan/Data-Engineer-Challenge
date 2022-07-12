package geo;
import java.sql.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URI;
import org.apache.commons.lang3.StringUtils;
import org.json.*;

public class geoConn {

	public static void main(String[] args) {

					Connection connection = null;
					try
					{		
						Class.forName("com.mysql.cj.jdbc.Driver");
						
						//charger la classe driver
						connection =DriverManager.getConnection("jdbc:mysql://localhost:3306/dataengineer?useSSL=false", "root", "");
						// créer l'objet statement (commande sql)instruction Sql
						Statement stmt = connection.createStatement();
						
						/*
						stmt.execute("ALTER TABLE address " + 
						          "ADD COLUMN longitude float, " + 
						          "ADD COLUMN `latitude` float");
						*/
						
						
						String sql = "SELECT address_id, address, city, postal_code FROM address";
						ResultSet res = stmt.executeQuery(sql);
						//extraire les données
						while(res.next()){
							int address_id = res.getInt("address_id");
							String address = res.getString("address");
							String city = res.getString("city");
							int postal_code = res.getInt("postal_code");
							String postaleStr = StringUtils.leftPad(String.valueOf(postal_code), 5, "0");
							String urlString = "https://nominatim.openstreetmap.org/search?postalcode=" + postaleStr + "&format=json";
							URL url = new URL(urlString);
							URI uri = new URI(url.getProtocol(), url.getUserInfo(), url.getHost(), url.getPort(), url.getPath(), url.getQuery(), url.getRef());
							url = uri.toURL();
							HttpURLConnection conn = (HttpURLConnection) url.openConnection();
							conn.setRequestMethod("GET");
							conn.setRequestProperty("Accept","application/json");	
							BufferedReader br = new BufferedReader(new InputStreamReader(
									(conn.getInputStream())));
							String output = null;
							output = br.readLine();
							String lon = null;
							String lat = null;
							if(output != null){
								JSONArray jsonArray = new JSONArray(output);
								for (int i = 0; i < jsonArray.length(); i++)
								{
									var item = jsonArray.getJSONObject(i);
									String displayName = item.getString("display_name");
									if(displayName.contains("France"))
									{
										lon = item.getString("lon");
										lat = item.getString("lat");
										break;
									}
								}
							}
							
							String query = "update address set longitude = ?, latitude = ? where address_id = ?";
						      PreparedStatement preparedStmt = connection.prepareStatement(query);
						      preparedStmt.setFloat(1, Float.parseFloat(lon));
						      preparedStmt.setFloat(2, Float.parseFloat(lat));
						      preparedStmt.setInt(3, address_id);
						      // execute update
						      preparedStmt.executeUpdate();
							
					        System.out.println("ID: " + address_id+"  "+"Adresse: " +address+" City: " + city+" Code_postale: " + postal_code + ", Lon=" + lon + ", Lat="+ lat);	
						}
						
						
						connection.close();
						
					 }
					catch(Exception e){ 
						System.out.println(e);
					}
	}
}
