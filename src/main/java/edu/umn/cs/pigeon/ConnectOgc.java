/*******************************************************************
 * Copyright (C) 2014 by Regents of the University of Minnesota. * * This Software is released under the Apache License,
 * Version 2.0 * http://www.apache.org/licenses/LICENSE-2.0 *
 *******************************************************************/
package edu.umn.cs.pigeon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import com.esri.core.geometry.ogc.OGCConcreteGeometryCollection;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCGeometryCollection;
import com.esri.core.geometry.ogc.OGCLineString;

/**
 * Connects together a set of Linestrings to form a longer Linestrings.
 * 
 * @author Ajinkya
 */
public class ConnectOgc extends EvalFunc<DataByteArray> {

    private ESRIGeometryParser geometryParser = new ESRIGeometryParser();

    @Override
    public DataByteArray exec(Tuple b) throws IOException {
        try {
            Iterator<Tuple> shapesIter = ((DataBag)b.get(0)).iterator();
            ArrayList<OGCLineString> linestrings = new ArrayList<OGCLineString>();

            while (shapesIter.hasNext()) {
                Tuple next = shapesIter.next();
                OGCGeometry geom = geometryParser.parseGeom(next.get(0));
                if (geom.isEmpty()) {
                    // Skip empty geometries
                } else if (geom instanceof OGCLineString) {
                    linestrings.add((OGCLineString)geom);
                } else {
                    throw new GeoException("Cannot connect shapes of type " + geom.getClass());
                }

            }

            System.out.println("*************************** I/P parsing done : I/P size :" + linestrings.size());

            List<OGCGeometry> aggregatedLines = new ArrayList<OGCGeometry>();
            aggregatedLines.add(linestrings.get(0));
            for (int j = 1; j < linestrings.size(); j++) {
                boolean isNonTochingLine = true;
                for (int k = 0; k < aggregatedLines.size(); k++) {
                    if (aggregatedLines.get(k).touches(linestrings.get(j))) {
                        OGCGeometry geometry = aggregatedLines.get(k);
                        OGCGeometry union = geometry.union(linestrings.get(j));
                        aggregatedLines.remove(k);
                        aggregatedLines.add(k, union);
                        isNonTochingLine = false;
                        break;
                    }
                }
                if (isNonTochingLine)
                    aggregatedLines.add(linestrings.get(j));
            }

            System.out.println("**************************** Aggragated geometries\n\n");
            for (OGCGeometry geometry : aggregatedLines) {
                System.out.println("Insert into public.lines(geom) values (ST_GeomFromText('" + geometry.asText() + "');");
            }

            System.out.println(
                "*************************** Aggragation completed. Aggragated line size :" + aggregatedLines.size());
            OGCGeometryCollection collection =
                new OGCConcreteGeometryCollection(aggregatedLines, aggregatedLines.get(0).getEsriSpatialReference());

            return new DataByteArray(collection.asBinary().array());
        } catch (Exception e) {
            throw new GeoException(e);
        }
    }
}
