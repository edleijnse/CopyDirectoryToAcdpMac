package leijnse.info;


import com.thebuzzmedia.exiftool.ExifTool;
import com.thebuzzmedia.exiftool.ExifToolBuilder;
import com.thebuzzmedia.exiftool.Tag;
import com.thebuzzmedia.exiftool.core.StandardTag;
import com.thebuzzmedia.exiftool.exceptions.UnsupportedFeatureException;
import com.thebuzzmedia.exiftool.logs.Logger;
import com.thebuzzmedia.exiftool.logs.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static java.util.Arrays.asList;

public class ExtractPictureMetaData {
    // 20180715 ExifTool Wrapper build in, supports CR3 Format
    // https://github.com/mjeanroy/exiftool/blob/master/README.md

    private static final Logger log = LoggerFactory.getLogger(ExtractPictureMetaData.class);


    private static ExifTool exifTool;

    static {
        try {
            exifTool = new ExifToolBuilder().withPoolSize(10).enableStayOpen().build();
        } catch (UnsupportedFeatureException ex) {
            // Fallback to simple exiftool instance.
            exifTool = new ExifToolBuilder().build();
        }
    }



    public static Map<Tag, String> parseFromExif(File image) throws Exception {
        // ExifTool path must be defined as a system property (`exiftool.path`),
        // but path can be set using `withPath` method
        return exifTool.getImageMeta(image, asList(
                StandardTag.ISO,
                StandardTag.APERTURE,
                StandardTag.WHITE_BALANCE,
                StandardTag.BRIGHTNESS,
                StandardTag.CONTRAST,
                StandardTag.SATURATION,
                StandardTag.SHARPNESS,
                StandardTag.SHUTTER_SPEED,
                StandardTag.DIGITAL_ZOOM_RATIO,
                StandardTag.IMAGE_WIDTH,
                StandardTag.IMAGE_HEIGHT,
                StandardTag.X_RESOLUTION,
                StandardTag.Y_RESOLUTION,
                StandardTag.FLASH,
                StandardTag.METERING_MODE,
                StandardTag.FOCAL_LENGTH,
                StandardTag.FOCAL_LENGTH_35MM,
                StandardTag.EXPOSURE_TIME,
                StandardTag.EXPOSURE_COMPENSATION,
                StandardTag.EXPOSURE_PROGRAM,
                StandardTag.ORIENTATION,
                StandardTag.COLOR_SPACE,
                StandardTag.SENSING_METHOD,
                StandardTag.SOFTWARE,
                StandardTag.MAKE,
                StandardTag.MODEL,
                StandardTag.LENS_MAKE,
                StandardTag.LENS_MODEL,
                StandardTag.OWNER_NAME,
                StandardTag.TITLE,
                StandardTag.AUTHOR,
                StandardTag.SUBJECT,
                StandardTag.KEYWORDS,
                StandardTag.COMMENT,
                StandardTag.RATING,
                StandardTag.RATING_PERCENT,
                StandardTag.DATE_TIME_ORIGINAL,
                StandardTag.GPS_LATITUDE,
                StandardTag.GPS_LATITUDE_REF,
                StandardTag.GPS_LONGITUDE,
                StandardTag.GPS_LONGITUDE_REF,
                StandardTag.GPS_ALTITUDE,
                StandardTag.GPS_ALTITUDE_REF,
                StandardTag.GPS_SPEED,
                StandardTag.GPS_SPEED_REF,
                StandardTag.GPS_PROCESS_METHOD,
                StandardTag.GPS_BEARING,
                StandardTag.GPS_BEARING_REF,
                StandardTag.GPS_TIMESTAMP,
                StandardTag.ROTATION,
                StandardTag.EXIF_VERSION,
                StandardTag.LENS_ID,
                StandardTag.COPYRIGHT,
                StandardTag.ARTIST,
                StandardTag.SUB_SEC_TIME_ORIGINAL,
                StandardTag.OBJECT_NAME,
                StandardTag.CAPTION_ABSTRACT,
                StandardTag.CREATOR,
                StandardTag.IPTC_KEYWORDS,
                StandardTag.COPYRIGHT_NOTICE,
                StandardTag.FILE_TYPE,
                StandardTag.FILE_SIZE,
                StandardTag.AVG_BITRATE,
                StandardTag.AVG_BITRATE,
                StandardTag.CREATE_DATE

        ));

    }


    public static PictureMetaData getPictureMetaDataExif(File file) throws IOException {
        // https://github.com/mjeanroy/exiftool


        PictureMetaData myPictureMetadata = new PictureMetaData();
        myPictureMetadata.setPictureName(Optional.of(file.getName()));
        myPictureMetadata.setAbsolutePath(Optional.of(file.getAbsolutePath()));
        myPictureMetadata.setCanonicalPath(Optional.of(file.getCanonicalPath()));
        // ExifTool path must be defined as a system property (`exiftool.path`),
        // but path can be set using `withPath` method.
        // KEYWORDS: to be checked (export from Lightroom)
        try {
            Map<Tag, String> myTags = parseFromExif(file);
            if (myTags != null) {
                myTags.forEach((tag, s) -> {
                    // System.out.println((tag.getName() + ": " + s));
                    s = s.replaceAll(",", ".");
                    if (tag.getName() == StandardTag.MAKE.getName()) {
                        myPictureMetadata.setMake(Optional.of(s));
                        myPictureMetadata.setMAKE(Optional.of(s));
                    } else if (tag.getName() == StandardTag.MODEL.getName()) {
                        myPictureMetadata.setModel(Optional.of(s));
                        myPictureMetadata.setMODEL(Optional.of(s));
                    } else if (tag.getName() == StandardTag.LENS_MODEL.getName()) {
                        myPictureMetadata.setLenseDescription(Optional.of(s));
                        myPictureMetadata.setLENS_MODEL(Optional.of(s));
                    } else if (tag.getName() == StandardTag.LENS_ID.getName()) {
                        myPictureMetadata.setLENS_ID(Optional.of(s));
                    } else if (tag.getName() == StandardTag.CREATE_DATE.getName()) {
                        myPictureMetadata.setDateTime(Optional.of(s));
                        myPictureMetadata.setCREATE_DATE(Optional.of(s));
                    } else if (tag.getName() == StandardTag.IMAGE_HEIGHT.getName()) {
                        myPictureMetadata.setHeight(Optional.of(s));
                        myPictureMetadata.setIMAGE_HEIGHT(Optional.of(s));
                    } else if (tag.getName() == StandardTag.IMAGE_WIDTH.getName()) {
                        myPictureMetadata.setWidth(Optional.of(s));
                        myPictureMetadata.setIMAGE_WIDTH(Optional.of(s));
                    } else if (tag.getName() == StandardTag.ISO.getName()) {
                        myPictureMetadata.setIso(Optional.of(s));
                        myPictureMetadata.setISO(Optional.of(s));
                    } else if (tag.getName() == StandardTag.SHUTTER_SPEED.getName()) {
                        myPictureMetadata.setExposure(Optional.of(s));
                        myPictureMetadata.setSHUTTER_SPEED(Optional.of(s));
                    } else if (tag.getName() == StandardTag.APERTURE.getName()) {
                        myPictureMetadata.setAperture(Optional.of(s));
                        myPictureMetadata.setAPERTURE(Optional.of(s));
                    } else if (tag.getName() == StandardTag.EXPOSURE_COMPENSATION.getName()) {
                        myPictureMetadata.setExposureBias((Optional.of(s)));
                        myPictureMetadata.setEXPOSURE_COMPENSATION((Optional.of(s)));
                    } else if (tag.getName() == StandardTag.FOCAL_LENGTH.getName()) {
                        myPictureMetadata.setFocalLength(Optional.of(s));
                        myPictureMetadata.setFOCAL_LENGTH(Optional.of(s));
                    } else if (tag.getName() == StandardTag.ISO.getName()) {
                        myPictureMetadata.setISO(Optional.of(s));
                    } else if (tag.getName() == StandardTag.APERTURE.getName()) {
                        myPictureMetadata.setAPERTURE(Optional.of(s));
                    } else if (tag.getName() == StandardTag.WHITE_BALANCE.getName()) {
                        myPictureMetadata.setWHITE_BALANCE(Optional.of(s));
                    } else if (tag.getName() == StandardTag.BRIGHTNESS.getName()) {
                        myPictureMetadata.setBRIGHTNESS(Optional.of(s));
                    } else if (tag.getName() == StandardTag.CONTRAST.getName()) {
                        myPictureMetadata.setCONTRAST(Optional.of(s));
                    } else if (tag.getName() == StandardTag.SATURATION.getName()) {
                        myPictureMetadata.setSATURATION(Optional.of(s));
                    } else if (tag.getName() == StandardTag.SHARPNESS.getName()) {
                        myPictureMetadata.setSHARPNESS(Optional.of(s));
                    } else if (tag.getName() == StandardTag.SHUTTER_SPEED.getName()) {
                        myPictureMetadata.setSHUTTER_SPEED(Optional.of(s));
                    } else if (tag.getName() == StandardTag.DIGITAL_ZOOM_RATIO.getName()) {
                        myPictureMetadata.setDIGITAL_ZOOM_RATIO(Optional.of(s));
                    } else if (tag.getName() == StandardTag.IMAGE_WIDTH.getName()) {
                        myPictureMetadata.setIMAGE_WIDTH(Optional.of(s));
                    } else if (tag.getName() == StandardTag.IMAGE_HEIGHT.getName()) {
                        myPictureMetadata.setIMAGE_HEIGHT(Optional.of(s));
                    } else if (tag.getName() == StandardTag.X_RESOLUTION.getName()) {
                        myPictureMetadata.setX_RESOLUTION(Optional.of(s));
                    } else if (tag.getName() == StandardTag.Y_RESOLUTION.getName()) {
                        myPictureMetadata.setY_RESOLUTION(Optional.of(s));
                    } else if (tag.getName() == StandardTag.FLASH.getName()) {
                        myPictureMetadata.setFLASH(Optional.of(s));
                    } else if (tag.getName() == StandardTag.METERING_MODE.getName()) {
                        myPictureMetadata.setMETERING_MODE(Optional.of(s));
                    } else if (tag.getName() == StandardTag.FOCAL_LENGTH.getName()) {
                        myPictureMetadata.setFOCAL_LENGTH(Optional.of(s));
                    } else if (tag.getName() == StandardTag.FOCAL_LENGTH_35MM.getName()) {
                        myPictureMetadata.setFOCAL_LENGTH_35MM(Optional.of(s));
                    } else if (tag.getName() == StandardTag.EXPOSURE_TIME.getName()) {
                        myPictureMetadata.setEXPOSURE_TIME(Optional.of(s));
                    } else if (tag.getName() == StandardTag.EXPOSURE_COMPENSATION.getName()) {
                        myPictureMetadata.setEXPOSURE_COMPENSATION(Optional.of(s));
                    } else if (tag.getName() == StandardTag.EXPOSURE_PROGRAM.getName()) {
                        myPictureMetadata.setEXPOSURE_PROGRAM(Optional.of(s));
                    } else if (tag.getName() == StandardTag.ORIENTATION.getName()) {
                        myPictureMetadata.setORIENTATION(Optional.of(s));
                    } else if (tag.getName() == StandardTag.COLOR_SPACE.getName()) {
                        myPictureMetadata.setCOLOR_SPACE(Optional.of(s));
                    } else if (tag.getName() == StandardTag.SENSING_METHOD.getName()) {
                        myPictureMetadata.setSENSING_METHOD(Optional.of(s));
                    } else if (tag.getName() == StandardTag.SOFTWARE.getName()) {
                        myPictureMetadata.setSOFTWARE(Optional.of(s));
                    } else if (tag.getName() == StandardTag.MAKE.getName()) {
                        myPictureMetadata.setMAKE(Optional.of(s));
                    } else if (tag.getName() == StandardTag.MODEL.getName()) {
                        myPictureMetadata.setMODEL(Optional.of(s));
                    } else if (tag.getName() == StandardTag.LENS_MAKE.getName()) {
                        myPictureMetadata.setLENS_MAKE(Optional.of(s));
                    } else if (tag.getName() == StandardTag.LENS_MODEL.getName()) {
                        myPictureMetadata.setLENS_MODEL(Optional.of(s));
                    } else if (tag.getName() == StandardTag.OWNER_NAME.getName()) {
                        myPictureMetadata.setOWNER_NAME(Optional.of(s));
                    } else if (tag.getName() == StandardTag.TITLE.getName()) {
                        myPictureMetadata.setTITLE(Optional.of(s));
                    } else if (tag.getName() == StandardTag.AUTHOR.getName()) {
                        myPictureMetadata.setAUTHOR(Optional.of(s));
                    } else if (tag.getName() == StandardTag.SUBJECT.getName()) {
                        myPictureMetadata.setSUBJECT(Optional.of(s));
                    } else if (tag.getName() == StandardTag.KEYWORDS.getName()) {
                        myPictureMetadata.setKEYWORDS(Optional.of(s));
                    } else if (tag.getName() == StandardTag.COMMENT.getName()) {
                        myPictureMetadata.setCOMMENT(Optional.of(s));
                    } else if (tag.getName() == StandardTag.RATING.getName()) {
                        myPictureMetadata.setRATING(Optional.of(s));
                    } else if (tag.getName() == StandardTag.RATING_PERCENT.getName()) {
                        myPictureMetadata.setRATING_PERCENT(Optional.of(s));
                    } else if (tag.getName() == StandardTag.DATE_TIME_ORIGINAL.getName()) {
                        myPictureMetadata.setDATE_TIME_ORIGINAL(Optional.of(s));
                    } else if (tag.getName() == StandardTag.GPS_LATITUDE.getName()) {
                        myPictureMetadata.setGPS_LATITUDE(Optional.of(s));
                    } else if (tag.getName() == StandardTag.GPS_LATITUDE_REF.getName()) {
                        myPictureMetadata.setGPS_LATITUDE_REF(Optional.of(s));
                    } else if (tag.getName() == StandardTag.GPS_LONGITUDE.getName()) {
                        myPictureMetadata.setGPS_LONGITUDE(Optional.of(s));
                    } else if (tag.getName() == StandardTag.GPS_LONGITUDE_REF.getName()) {
                        myPictureMetadata.setGPS_LONGITUDE_REF(Optional.of(s));
                    } else if (tag.getName() == StandardTag.GPS_ALTITUDE.getName()) {
                        myPictureMetadata.setGPS_ALTITUDE(Optional.of(s));
                    } else if (tag.getName() == StandardTag.GPS_ALTITUDE_REF.getName()) {
                        myPictureMetadata.setGPS_ALTITUDE_REF(Optional.of(s));
                    } else if (tag.getName() == StandardTag.GPS_SPEED.getName()) {
                        myPictureMetadata.setGPS_SPEED(Optional.of(s));
                    } else if (tag.getName() == StandardTag.GPS_SPEED_REF.getName()) {
                        myPictureMetadata.setGPS_SPEED_REF(Optional.of(s));
                    } else if (tag.getName() == StandardTag.GPS_PROCESS_METHOD.getName()) {
                        myPictureMetadata.setGPS_PROCESS_METHOD(Optional.of(s));
                    } else if (tag.getName() == StandardTag.GPS_BEARING.getName()) {
                        myPictureMetadata.setGPS_BEARING(Optional.of(s));
                    } else if (tag.getName() == StandardTag.GPS_BEARING_REF.getName()) {
                        myPictureMetadata.setGPS_BEARING_REF(Optional.of(s));
                    } else if (tag.getName() == StandardTag.GPS_TIMESTAMP.getName()) {
                        myPictureMetadata.setGPS_TIMESTAMP(Optional.of(s));
                    } else if (tag.getName() == StandardTag.ROTATION.getName()) {
                        myPictureMetadata.setROTATION(Optional.of(s));
                    } else if (tag.getName() == StandardTag.EXIF_VERSION.getName()) {
                        myPictureMetadata.setEXIF_VERSION(Optional.of(s));
                    } else if (tag.getName() == StandardTag.LENS_ID.getName()) {
                        myPictureMetadata.setLENS_ID(Optional.of(s));
                    } else if (tag.getName() == StandardTag.COPYRIGHT.getName()) {
                        myPictureMetadata.setCOPYRIGHT(Optional.of(s));
                    } else if (tag.getName() == StandardTag.ARTIST.getName()) {
                        myPictureMetadata.setARTIST(Optional.of(s));
                    } else if (tag.getName() == StandardTag.SUB_SEC_TIME_ORIGINAL.getName()) {
                        myPictureMetadata.setSUB_SEC_TIME_ORIGINAL(Optional.of(s));
                    } else if (tag.getName() == StandardTag.OBJECT_NAME.getName()) {
                        myPictureMetadata.setOBJECT_NAME(Optional.of(s));
                    } else if (tag.getName() == StandardTag.CAPTION_ABSTRACT.getName()) {
                        myPictureMetadata.setCAPTION_ABSTRACT(Optional.of(s));
                    } else if (tag.getName() == StandardTag.CREATOR.getName()) {
                        myPictureMetadata.setCREATOR(Optional.of(s));
                    } else if (tag.getName() == StandardTag.IPTC_KEYWORDS.getName()) {
                        myPictureMetadata.setIPTC_KEYWORDS(Optional.of(s));
                    } else if (tag.getName() == StandardTag.COPYRIGHT_NOTICE.getName()) {
                        myPictureMetadata.setCOPYRIGHT_NOTICE(Optional.of(s));
                    } else if (tag.getName() == StandardTag.FILE_TYPE.getName()) {
                        myPictureMetadata.setFILE_TYPE(Optional.of(s));
                    } else if (tag.getName() == StandardTag.FILE_SIZE.getName()) {
                        myPictureMetadata.setFILE_SIZE(Optional.of(s));
                    } else if (tag.getName() == StandardTag.AVG_BITRATE.getName()) {
                        myPictureMetadata.setAVG_BITRATE(Optional.of(s));
                    } else if (tag.getName() == StandardTag.MIME_TYPE.getName()) {
                        myPictureMetadata.setMIME_TYPE(Optional.of(s));
                    } else if (tag.getName() == StandardTag.CREATE_DATE.getName()) {
                        myPictureMetadata.setCREATE_DATE(Optional.of(s));
                    }
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


        return myPictureMetadata;
    }


}
