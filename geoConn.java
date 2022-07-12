package geo;
import java.sql.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import org.apache.commons.lang3.StringUtils;
import org.json.*;

public class geoConn {

	public static void main(String[] args) {

		Connection connectionBd = null;
		try
		{	
			//charger la classe driver
			// faut rajouter le bibliotheque Mysql.Connector
			Class.forName("com.mysql.cj.jdbc.Driver");					

			//Connexion à la BD dataengineer
			connectionBd = DriverManager.getConnection("jdbc:mysql://localhost:3306/dataengineer?useSSL=false", "root", "");
			
			
			// créer l'objet statement (commande sql) instruction Sql
			Statement stmt = connectionBd.createStatement();
			
			//A executer une seule fois
			/*
			stmt.execute("ALTER TABLE address " + 
			          "ADD COLUMN longitude float, " + 
			          "ADD COLUMN `latitude` float");
				*/
			
			String sql = "SELECT address_id, address, city, postal_code FROM address";
			ResultSet res = stmt.executeQuery(sql);
			
			//extraire les données
			while(res.next()){
				//Extraire les valeurs d'une ligne
				int address_id = res.getInt("address_id");
				String address = res.getString("address");
				String city = res.getString("city");
				int postal_code = res.getInt("postal_code");
				
				//Pour gerer le code postale < 5 chiffres => par exmple un code postale "7451", doit etre transformer en "07451" pour qu'il soit geré par l' API
				String postaleEnString = StringUtils.leftPad(String.valueOf(postal_code), 5, "0");
				
				//Preparer l'URL de l'API
				String urlString = "https://nominatim.openstreetmap.org/search?postalcode=" + postaleEnString + "&format=json";
				URL url = new URL(urlString);
				
				HttpURLConnection connexionAPI = (HttpURLConnection) url.openConnection();
				connexionAPI.setRequestMethod("GET");
				connexionAPI.setRequestProperty("Accept","application/json");
				
				BufferedReader br = new BufferedReader(new InputStreamReader((connexionAPI. getInputStream())));
				
				String output = br.readLine();
				String lon = null;
				String lat = null;
				if(output != null){
					JSONArray jsonArray = new JSONArray(output);
					for (int i = 0; i < jsonArray.length(); i++)
					{
						JSONObject element = jsonArray.getJSONObject(i); // equivalent à T[i]
						String displayName = element.getString("display_name"); // lire display_name de l'element
						if(displayName.contains("France"))
						{
							lon = element.getString("lon");
							lat = element.getString("lat");
							break; // sortir de boucle une fois trouvé l'information "France"
						}
					}
				}
				
				//Mettre à jour notre BD avec les données recuperé à partir de l'API
				//à l'aide de parametres
				String query = "update address set longitude = ?, latitude = ? where address_id = ?";
		        PreparedStatement updateQuery = connectionBd.prepareStatement(query);
		        //affecter les valeurs au parametres
		        updateQuery.setFloat(1, Float.parseFloat(lon));
		        updateQuery.setFloat(2, Float.parseFloat(lat));
		        updateQuery.setInt(3, address_id);
		        // executer l'update
		        updateQuery.executeUpdate();
				
		        //Affichage Console
		        System.out.println("ID: " + address_id+"  "+"Adresse: " +address+" City: " + city+" Code_postale: " + postal_code + ", Lon=" + lon + ", Lat="+ lat);	
			}				
			connectionBd.close();
			
		 }
		catch(Exception e){ 
			System.out.println(e);
		}
	}
}
