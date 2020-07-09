package lee.high.stream;

import org.junit.Test;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class OsVersionTest {

    @Test
    public void 버전체크_테스트() {
        final OsVersion requestIosVersion = OsVersion.of("2.1", "ios");
        assertTrue(!OsVersion.PASS_IOS_VERSION.graterThan(requestIosVersion));
        assertTrue(OsVersion.PASS_IOS_VERSION.lessThan(requestIosVersion));

        final OsVersion requestAndVersion = OsVersion.of("5.1", "android");
        assertTrue(OsVersion.PASS_ANDROID_VERSION.graterThan(requestAndVersion));
        assertTrue(!OsVersion.PASS_ANDROID_VERSION.lessThan(requestAndVersion));
    }

    @Test
    public void 로직_테스트() {
        final OsVersion requestVersion = OsVersion.of("2.1", "ios");
        final OsVersion passVersion = requestVersion.os == OsVersion.OS.ANDROID
                ? OsVersion.PASS_ANDROID_VERSION
                : OsVersion.PASS_IOS_VERSION;

        assertTrue(!passVersion.graterThan(requestVersion));
    }

    static class OsVersion {
        public static OsVersion PASS_ANDROID_VERSION = new OsVersion(4, 2, OS.ANDROID);
        public static OsVersion PASS_IOS_VERSION = new OsVersion(2, 2, OS.IOS);

        private int major;
        private int minor;
        private OS os;

        public static OsVersion of(String version, String os) {
            return new OsVersion(version, os);
        }

        public OsVersion(int major, int minor, OS os) {
            this.major = major;
            this.minor = minor;
            this.os = os;
        }

        public OsVersion(String version, String os) {
            if(version == null || version.isEmpty()) {
                /*
                에러
                 */
            }
            final String[] split = version.split("[.]");

            if(split.length != 2) {
                /*
                에러
                 */
            }

            try {
                major = Integer.parseInt(split[0]);
                minor = Integer.parseInt(split[1]);
            } catch (Exception e) {
                /*
                파싱에러
                 */
            }

            this.os = OS.of(os);
        }

        public boolean graterThan(OsVersion osVersion) {
            if(this.os == osVersion.os) {
                if (this.major < osVersion.major) {
                    return true;
                } else if (this.major == osVersion.major) {
                    if (this.minor < osVersion.minor) {
                        return true;
                    }
                }
            }

            return false;
        }

        public boolean graterEqualThan(OsVersion osVersion) {
            if(this.os == osVersion.os) {
                if (this.major < osVersion.major) {
                    return true;
                } else if (this.major == osVersion.major) {
                    if (this.minor <= osVersion.minor) {
                        return true;
                    }
                }
            }

            return false;
        }

        public boolean lessThan(OsVersion osVersion) {
            if(this.os == osVersion.os) {
                if (this.major > osVersion.major) {
                    return true;
                } else if (this.major == osVersion.major) {
                    if (this.minor > osVersion.minor) {
                        return true;
                    }
                }
            }

            return false;
        }

        enum OS {
            ANDROID("android"),
            IOS("ios");

            private final String value;

            private static final Map<String, OS> map = Stream.of(OS.values())
                    .collect(Collectors.toMap(i -> i.value, i -> i));

            OS(String value) {
                this.value = value;
            }

            public static OS of(String value) {
                final OS os = map.get(value);
                if(os == null) {
                    /*
                    에러
                     */
                }
                return os;
            }
        }

    }
}
