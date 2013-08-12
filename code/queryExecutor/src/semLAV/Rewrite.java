package semLAV;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static ch.lambdaj.Lambda.*;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.hamcrest.Matchers.*;

public class Rewrite {

	Predicate head;
	ArrayList<Predicate> goals;


	public Rewrite(Predicate h, ArrayList<Predicate> g){
		this.head = h;
		this.goals = g;
	}

	// Construct a Rewrite from a String of the form
	// Head(X,Y..) :- SG(X,Y..),SG(W,Z)...
	public Rewrite(String str){
		String[] split = str.split(" :- ");
		this.head = new Predicate(split[0]);
		this.goals = new ArrayList<Predicate>();
        Pattern goal = Pattern.compile("\\w+\\((\\w+(,(\\s)?)?)+\\)");
        Matcher match = goal.matcher(split[1]);
		while(match.find()){
			this.goals.add(new Predicate(match.group()));
		}
	}

	public ArrayList<Predicate> getGoals(){
		return this.goals;
	}

    public Predicate getHead() {
        return this.head;
    }

	@Override
	public boolean equals(Object obj){
		if(obj instanceof Rewrite){
			Rewrite r = (Rewrite) obj;
			return this.head.equals(r.head) && this.goals.equals(r.goals);
		}else {return false;}
	}

	@Override
	public int hashCode() {
		int hash = 7;
		hash = 19 * hash + (this.head != null ? this.head.hashCode() : 0);
		hash = 19 * hash + (this.goals != null ? this.goals.hashCode() : 0);
		return hash;
	}


	@Override
	public String toString(){
		String str = "";
		str = str.concat(this.head.toString());
		str = str.concat(" :- ");
		for(Predicate goal : goals){
			str = str.concat(goal.toString());
			str = str.concat(",");

		}
		// Return without the last extra comma
		return str.substring(0,str.length()-1);

	}

	public int getPredicateNumber(){
		return this.goals.size();
	}
}
