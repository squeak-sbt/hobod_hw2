package ru.mipt.hobod.hw2.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.SAXException;
import ru.mipt.hobod.hw2.entity.TaggedKey;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Created by dmitry on 26.03.17.
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder documentBuilder;
        Document document = null;
        try {
            documentBuilder = builderFactory.newDocumentBuilder();
            document = documentBuilder.parse(new ByteArrayInputStream(value.getBytes()));
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        }
        if (document == null) {
            return;
        }
        Element element = document.getElementById("row");
        if (element == null) {
            return;
        }

        if (!element.hasAttribute("Id")) {
            return;
        }

        String id = element.getAttribute("Id");

        if (element.hasAttribute("PostTypeId")) {
            if (element.hasAttribute("PostTypeId") || element.hasAttribute("Score") || element.hasAttribute("OwnerUserId")) {
                return;
            }
            int postTypeId = Integer.valueOf(element.getAttribute("PostTypeId"));
            int score = Integer.valueOf(element.getAttribute("Score"));
            String ownerUserId = element.getAttribute("OwnerUserId");
            if (postTypeId == 2) { //answer
                if (element.hasAttribute("ParentId")) {
                    String parentId = element.getAttribute("ParentId");
                    Text outputKey = new Text(ownerUserId);
                    Text outputValue = new Text();
                    outputValue.set("2," + id + "," + score + "," + parentId);
                    context.write(outputKey, outputValue);
                }
            }
            else { //question
                Text outputKey = new Text(ownerUserId);
                Text outputValue = new Text();
                outputValue.set("1," + id);
                context.write(outputKey, outputValue);
            }
        }
        else if (element.hasAttribute("Reputation")) { //It is user
            Text outputKey = new Text(id);
            Text outputValue = new Text();
            outputValue.set("3," + element.getAttribute("Reputation"));
            context.write(outputKey, outputValue);
        }

    }


}
