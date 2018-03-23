package Tools;

import java.util.Comparator;

public class MComparator implements Comparator<String> {

	@Override
	public int compare(String k1, String k2) {

		k1 = k1.split(",")[2];
		k2 = k2.split(",")[2];
		if (Integer.parseInt(k2.toString()) - Integer.parseInt(k1.toString()) == 0) {
			return -1;
		} else {
			return Integer.parseInt(k2.toString())- Integer.parseInt(k1.toString());
		}
	}

}
