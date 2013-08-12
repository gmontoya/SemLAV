package semLAV;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.shared.PrefixMapping.Factory;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.Template;

import com.hp.hpl.jena.sparql.algebra.*;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.syntax.*; 
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.graph.Triple;

import com.hp.hpl.jena.sparql.core.BasicPattern;

import java.util.*;

class test1 {

    public static void main(String args[]) {

        String query = "SELECT * WHERE { ?a <http://predicate> ?b . FILTER (?a != ?b) }";
        //String model = "<http://subject1> <http://predicate> <http://object1>";
        Query q = QueryFactory.create(query);
        //Model m = ModelFactory.

        Op op = Algebra.compile(q);
        myVisitor123 mv = new myVisitor123();
        OpWalker ow = new OpWalker();
        ow.walk(op, mv);
        List<Triple> l = mv.getTriples();
        BasicPattern bgp = BasicPattern.wrap(l);
        Template pat = new Template(bgp);

        Element eg = q.getQueryPattern();
        Query nq = new Query();
        nq.setQueryConstructType();
        nq.setQueryPattern(eg);
        PrefixMapping p = PrefixMapping.Factory.create();
        p.setNsPrefixes(q.getPrefixMapping());
        nq.setPrefixMapping(p);
        nq.setResultVars();
        nq.setQueryPattern(eg);
        nq.setConstructTemplate(pat);
        System.out.println(nq);
    }
}
