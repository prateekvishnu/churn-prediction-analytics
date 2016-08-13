package Data_Preprocessing;

import java.util.*;

class Numbers {
	Random randnum;

	public Numbers() {
		randnum = new Random();

	}

	public int random(int i) {
		return randnum.nextInt(i);
	}
}
