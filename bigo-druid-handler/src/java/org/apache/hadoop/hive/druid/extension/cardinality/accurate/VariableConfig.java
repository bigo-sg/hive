package org.apache.hadoop.hive.druid.extension.cardinality.accurate;

/**
 * @author jiangshequan
 * @title: VariableConfig
 * @date 2019/12/12 12:40
 */
public class VariableConfig {
  public static String nameSpace;
  public static String openOneId;
  public static String oneIdUrl;
  public static final byte accurateCardinalityCacheTypeId = 0x41;
  public static final byte bitmapAggCacheTypeId = 0x42;

  public static void setNameSpace(String name)
  {
    nameSpace = name;
  }
  public static String getNameSpace()
  {
    return nameSpace;
  }

  public static void setOpenOneId(String open)
  {
    openOneId = open;
  }
  public static String getOpenOneId()
  {
    return openOneId;
  }

  public static void setOneIdUrl(String url) { oneIdUrl = url; }
  public static String getOneIdUrl() { return oneIdUrl; }
}
