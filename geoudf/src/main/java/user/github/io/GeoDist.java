package user.github.io;
import java.sql.SQLException;
/*

* Class to calculate geo-location distance
* 
* http://wiki.gis-lab.info/w/Вычисление_расстояния_и_начального_азимута_между_двумя_точками_на_сфере

*/
// https://community.hortonworks.com/articles/24531/geo-distance-calculations-in-hive-and-java.html
// https://community.hortonworks.com/articles/138964/how-to-use-udfs-to-run-hive-queries-in-ambari-hive.html
// расстояние выводится в метрах 
// Distance_A_B = .Atan2(Sin(.Pi() * Lat1 / 180) * Sin(.Pi() * Lat2 / 180) + Cos(.Pi() * Lat1 / 180) * Cos(.Pi() * Lat2 / 180) * Cos(Abs(.Pi() * Long2 / 180 - .Pi() * Long1 / 180)), _
//         ((Cos(.Pi() * Lat2 / 180) * Sin(.Pi() * Long2 / 180 - .Pi() * Long1 / 180)) ^ 2 + (Cos(.Pi() * Lat1 / 180) * Sin(.Pi() * Lat2 / 180) - Sin(.Pi() * Lat1 / 180) * Cos(.Pi() * Lat2 / 180) * Cos(Abs(.Pi() * Long2 / 180 - .Pi() * Long1 / 180))) ^ 2) ^ 0.5) * 6372795

public class GeoDist {
	public static double distance(double lat1, double lon1, double lat2, double lon2, String string) {
		double theta = lon1 - lon2;

		double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));

		dist = Math.acos(dist);

		dist = rad2deg(dist);

		dist = dist * 60 * 1.1515;

		if (string == "K") {

		dist = dist * 1.609344;

		} else if (string == "N") {

		dist = dist * 0.8684;

		}

		return (dist);
	}
	
	/*###################################################################*/

	/*# This function converts decimal degrees to radians #*/

	/*###################################################################*/

	private static double deg2rad(double deg) {
	    return (deg * Math.PI / 180.0);
	}
	
	/*###################################################################*/

	/*# This function converts radians to decimal degrees #*/

	/*###################################################################*/

	private static double rad2deg(double rad) {
	    return (rad * 180 / Math.PI);
	}
	
	public static void main(String[] args) throws SQLException {
		System.out.println(distance(42.4428, -71.2317, 37.405990600586, -122.07851409912, "M") + " Miles\n");

		// lexington to mountain view
		
		// 42.4428 -71.2317 37.405990600586 -122.07851409912
	}
}
