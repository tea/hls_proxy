#include <iostream>
#include <iomanip>

#include <boost/asio.hpp>
#include <boost/filesystem.hpp>
#define BOOST_LOG_DYN_LINK 1
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/attributes.hpp>
#include <boost/chrono.hpp>
#include <boost/program_options.hpp>

#include "urdl/url.hpp"
#include "urdl/istream.hpp"
#include "urdl/read_stream.hpp"

#define TRACE BOOST_LOG_TRIVIAL(trace) <<
#define DEBUG BOOST_LOG_TRIVIAL(debug) <<
#define INFO BOOST_LOG_TRIVIAL(info) <<
#define WARNING BOOST_LOG_TRIVIAL(warning) <<
#define ERROR BOOST_LOG_TRIVIAL(error) <<
#define FATAL BOOST_LOG_TRIVIAL(fatal) <<

using namespace std::placeholders;
using namespace boost::asio;
using namespace boost::filesystem;
using namespace boost::chrono;
using namespace urdl;

static const size_t download_buffer_size = 32768;
static boost::posix_time::seconds manifest_reload_period(0);
static size_t keep_segments = 50;
static size_t required_manifest_size;
static std::string ffmpeg_command;

static io_service ios;
static bool ffmpeg_started = false;

void new_manifest_size(size_t size)
{
    if (!ffmpeg_started && required_manifest_size <= size)
    {
        ffmpeg_started = true;
        INFO "Manifest is sufficiently large - starting ffmpeg";
        if (!fork()) {
            const char* const args[] = {
                "/bin/sh",
                "-c",
                ffmpeg_command.c_str(),
                NULL
            };
            execve(args[0], (char*const*)args, environ);
            std::cerr << "Failed to execute command!";
            exit(1);
        }
    }
}

static urdl::url absolutize(const urdl::url& parent, const std::string& url)
{
    boost::system::error_code ec;
    urdl::url new_url = urdl::url::from_string(url, ec);
    if (!ec)
        return new_url;
    path new_path = absolute(url, path(parent.path()).remove_filename());
    return urdl::url::from_string(parent.to_string(
                urdl::url::protocol_component  |
                urdl::url::user_info_component |
                urdl::url::host_component      |
                urdl::url::port_component)
                + new_path.generic_string());
}

class Segment
{
public:
    Segment() : buffer(download_buffer_size)
    {
    }
    ~Segment() {
        boost::system::error_code ec;
        segment_stream.close(ec);
        if (downloaded) {
            DEBUG "Removing " << filename;
            remove(filename, ec);
        }
    }

    void check_download()
    {
        if (downloaded || downloading)
            return;
        downloaded = downloading = true;
        path basename = path(file_url.path()).filename();
        this->basename = basename.generic_string();
        filename = absolute(basename);
        DEBUG "Downloading " << file_url.to_string() << " to " << filename;
        download_start = steady_clock::now();
        filesize = 0;
        file.open(filename.native(), std::ios::binary | std::ios::out);
        if (!file) {
            ERROR "Cannot create file " << filename;
            downloading = false;
            return;
        }
        segment_stream.async_open(file_url, [this] (const boost::system::error_code& ec) {
            if (ec) {
                ERROR "Error opening segment URL " << file_url.to_string() << ": " << ec.message();
                onerror();
            } else {
                segment_stream.async_read_some(boost::asio::buffer(buffer), std::bind(&Segment::readsome, this, _1, _2));
            }
        });
    }

    std::ofstream file;
    path filename;
    url file_url;
    std::string basename;
    std::vector<std::string> options;
    bool downloaded = false;
    bool downloading = false;
    bool complete = false;
private:
    void readsome(const boost::system::error_code& ec, std::size_t bytes_transferred)
    {
        if (ec == error::eof) {
            file.close();
            if (!file) {
                ERROR "Cannot write to file " << filename;
                onerror();
            } else if (filesize == 0) {
                ERROR "Empty segment " << file_url.to_string();
                onerror();
            } else {
                downloading = false;
                complete = true;
                auto download_duration = duration_cast<milliseconds>(steady_clock::now() - download_start).count();
                INFO "Completed segment " << file_url.to_string() << " (" << std::setprecision(1) << std::fixed <<
                    (filesize / 1024.0 / 1024.0) << " MB) in " << download_duration <<
                    " ms (" << (filesize * 8000 / 1024.0 / 1024.0 / download_duration) << "Mbps)";
            }
        } else if (ec) {
            ERROR "Error reading URL " << file_url.to_string() << ": " << ec.message();
            onerror();
        } else {
            file.write(&buffer[0], bytes_transferred);
            filesize += bytes_transferred;
            segment_stream.async_read_some(boost::asio::buffer(buffer), std::bind(&Segment::readsome, this, _1, _2));
        }
    }

    void onerror()
    {
        boost::system::error_code ec;
        segment_stream.close(ec);
        downloading = false;
        file.close();
        remove(filename, ec);
    }

    read_stream segment_stream { ios };
    std::vector<char> buffer;
    steady_clock::time_point download_start;
    size_t filesize = 0;
};

class ManifestReader
{
public:
    ManifestReader() : buffer(download_buffer_size)
    {
    }

    virtual ~ManifestReader()
    {
        boost::system::error_code ec;
        manifest_stream.close(ec);
    }

    void load_manifest(urdl::url manifest_url)
    {
        boost::system::error_code ec;
        manifest_stream.close(ec);

        DEBUG "Loading manifest " << manifest_url.to_string();

        this->manifest_url = manifest_url;
        manifest.clear();

        manifest_stream.async_open(manifest_url, [this] (const boost::system::error_code& ec) {
            if (ec) {
                ERROR "Error opening URL " << this->manifest_url.to_string() << ": " << ec.message();
                load_manifest(this->manifest_url);
            } else {
                manifest_stream.async_read_some(boost::asio::buffer(buffer), std::bind(&ManifestReader::readsome, this, _1, _2));
            }
        });
    }

protected:
    virtual bool parse_manifest(urdl::url& manifest_url, const std::string& manifest) = 0;

private:
    void readsome(const boost::system::error_code& ec, std::size_t bytes_transferred)
    {
        if (ec == error::eof) {
            if (parse_manifest(manifest_url, manifest)) {
                timer.expires_from_now(manifest_reload_period);
                timer.async_wait([this] (const boost::system::error_code&) {
                    load_manifest(manifest_url);
                });
            } else {
                load_manifest(manifest_url);
            }
        } else if (ec) {
            ERROR "Error reading URL " << manifest_url.to_string() << ": " << ec.message();
            load_manifest(manifest_url);
        } else {
            manifest.append(buffer.begin(), buffer.begin() + bytes_transferred);
            manifest_stream.async_read_some(boost::asio::buffer(buffer), std::bind(&ManifestReader::readsome, this, _1, _2));
        }
    }


    urdl::url manifest_url;
    read_stream manifest_stream { ios };
    deadline_timer timer { ios };
    std::vector<char> buffer;
    std::string manifest;
};

class HLSManifestReader : public ManifestReader
{
private:
    bool parse_manifest(urdl::url& manifest_url, const std::string& manifest) final
    {
        std::stringstream is(manifest);
        std::string line;
        std::vector<std::string> new_options;
        std::vector<std::string> segment_options;
        if (!std::getline(is, line) || line.find("#EXTM3U") != 0) {
            WARNING "Cannot parse manifest - bad header line";
            return false;
        }
        new_options.emplace_back(line);
        unsigned int seq_no = 0;
        bool is_master = false;
        while (std::getline(is, line)) {
            while (!line.empty() && (line.back() == '\r' || line.back() == '\n'))
                line.resize(line.size() - 1);
            if (line.find("#EXT-X-STREAM-INF:") == 0) {
                is_master = true;
            } else if (line.find("#EXT-X-TARGETDURATION") == 0) {
                if (is_master) {
                    ERROR "Unexpected line in master playlist: " << line;
                    return false;
                }
                if (seq_no)
                    WARNING "Ignoring line after segments: " << line;
                else
                    new_options.emplace_back(line);
            } else if (line.find("#EXT-X-MEDIA-SEQUENCE:") == 0) {
                if (is_master || seq_no) {
                    ERROR "Unexpected line in master playlist: " << line;
                    return false;
                }
                seq_no = std::stoi(line.substr(22));
            } else if (line.find("#EXT-X-PROGRAM-DATE-TIME:") == 0 ||
                       line.find("#EXTINF:") == 0) {
                if (is_master) {
                    ERROR "Unexpected line in master playlist: " << line;
                    return false;
                }
                segment_options.emplace_back(line);
            } else if (line[0] == '#') {
                WARNING "Unknown line in manifest: " << line;
            } else if (!line.empty()) {
                if (is_master) {
                    manifest_url = absolutize(manifest_url, line);
                    INFO "Selected variant manifest " << manifest_url.to_string();
                    return false;
                }
                if (!seq_no) {
                    WARNING "Ignoring line without sequence no: " << line;
                } else {
                    if (segments.find(seq_no) == segments.end()) {
                        auto seg = std::make_shared<Segment>();
                        segments.emplace(seq_no, seg);
                        seg->file_url = absolutize(manifest_url, line);
                        seg->options.swap(segment_options);
                    }
                    seq_no++;
                }
                segment_options.clear();
            }
        }
        if (is_master) {
            WARNING("No variants in master playlist!");
            return false;
        }
        if (!segment_options.empty())
            WARNING "Unclaimed segment options!";
        manifest_options.swap(new_options);
        on_manifest();
        return true;
    }

    void on_manifest()
    {
        for(auto& i : segments) {
            auto& segment = *i.second;
            segment.check_download();
        }
        while(!segments.empty() && (!segments.begin()->second->downloading && !segments.begin()->second->complete)) { // clean old failures
            segments.erase(segments.begin());
        }
        size_t count = 0;
        for(auto& i : segments) {
            auto& segment = *i.second;
            if (segment.downloading)
                break;
            ++count;
        }
        while(count > keep_segments) { // remove old downloaded/failed segments
            --count;
            segments.erase(segments.begin());
        }
        std::ofstream manifest("index.m3u8.new", std::ios::out);
        for(auto& opt : manifest_options)
            manifest << opt << "\r\n";
        count = 0;
        unsigned int first_seqno = 0, last_seqno = 0;
        for(auto& i : segments) { // write all (contiguous) downloaded/failed segments
            auto& segment = *i.second;
            if (segment.downloading)
                break;
            if (!first_seqno) {
                first_seqno = i.first;
                manifest << "#EXT-X-MEDIA-SEQUENCE:" << first_seqno << "\r\n";
            }
            last_seqno = i.first;
            for(auto& opt : segment.options)
                manifest << opt << "\r\n";
            manifest << segment.basename << "\r\n";
            if (segment.complete)
                ++count;
        }
        manifest.close();
        if (!manifest) {
            ERROR "Error writing new manifest file";
        } else {
            boost::system::error_code ec;
            rename("index.m3u8.new", "index.m3u8", ec);
            if (ec) {
                ERROR "Error renaming manifest file: " << ec;
            }
            else {
                DEBUG "New manifest seq# " << first_seqno << "-" << last_seqno << " (" << count << " completed)";
                new_manifest_size(count);
            }
        }
    }

    std::vector<std::string> manifest_options;
    std::map<unsigned int, std::shared_ptr<Segment>> segments;
};

class DownloaderManifestReader : public ManifestReader
{
private:
    bool parse_manifest(urdl::url& manifest_url, const std::string& manifest) final
    {
        std::stringstream is(manifest);
        std::string line;
        while (std::getline(is, line)) {
            while (!line.empty() && (line.back() == '\r' || line.back() == '\n'))
                line.resize(line.size() - 1);
            if (line.empty() || line[0] == '#')
                continue;
            auto file_url = absolutize(manifest_url, line);
            if (std::find_if(segments.begin(), segments.end(), [&file_url](std::shared_ptr<Segment>& seg) -> bool {return seg->file_url == file_url;}) == segments.end()) {
                auto seg = std::make_shared<Segment>();
                seg->file_url = file_url;
                segments.emplace_back(seg);
            }
        }
        on_manifest();
        return true;
    }

    void on_manifest()
    {
        for(auto& i : segments) {
            auto& segment = *i;
            segment.check_download();
        }
        size_t count = 0;
        for(auto& i : segments) {
            auto& segment = *i;
            if (segment.downloading)
                break;
            ++count;
        }
        while(count > keep_segments) { // remove old downloaded/failed segments
            --count;
            segments.erase(segments.begin());
        }
    }

    std::deque<std::shared_ptr<Segment>> segments;
};

int main(int argc, char **argv)
{
    using namespace boost::program_options;
    unsigned int reload_period;
    options_description desc("General");
    bool download_only;
    desc.add_options()
        ("verbose", "Increase console log verbosity")
        ("quiet", "Only report warnings to console")
        ("manifest_reload_period", value(&reload_period)->default_value(3), "How ofter to recheck manifest file")
        ("keep_segments", value(&keep_segments)->default_value(50), "Maximum number of segments in the manifest")
        ("required_manifest_size", value(&required_manifest_size)->default_value(6), "Number of segments to download before starting ffmpeg")
        ("ffmpeg_command", value<std::string>(), "Command line to start when enough segments are available")
        ("log_file", value<std::string>()->default_value("hls_proxy.log"), "Log file name")
        ("download_only", value(&download_only)->default_value(false), "Just download all the segments mentioned in the manifest")
        ;

    try {
        options_description hidden;
        hidden.add_options()
            ("source", value<std::string>()->required(), "HLS source URL")
            ;
        options_description all;
        all.add(desc).add(hidden);
        variables_map vm;
        parsed_options parsed = command_line_parser(argc, argv).
            options(all).
            positional(positional_options_description().add("source", 1)).
            run();
        store(parsed, vm);
        notify(vm);
        if (!download_only) {
            if (vm.count("ffmpeg_command") == 0) {
            FATAL "ffmpeg_command option required when operating in HLS mode";
            return 1;
            }
            ffmpeg_command = vm["ffmpeg_command"].as<std::string>();
        }

        manifest_reload_period = boost::posix_time::seconds(reload_period);

        using namespace boost::log;
        auto file_log = add_file_log (
            keywords::file_name = vm["log_file"].as<std::string>(),
            keywords::format = "[%TimeStamp%]: %Message%",
            keywords::auto_flush = true
        );
        auto console = add_console_log();
        if (vm.count("verbose"))
            console->set_filter ( trivial::severity >= trivial::debug );
        else if (vm.count("quiet"))
            console->set_filter ( trivial::severity >= trivial::warning);
        else
            console->set_filter ( trivial::severity >= trivial::info);
        core::get()->add_global_attribute("TimeStamp", attributes::local_clock());

        std::string url  = vm["source"].as<std::string>();
        INFO "Starting " << url;
        if (download_only) {
            DownloaderManifestReader manifest_reader;
            manifest_reader.load_manifest(url);
            ios.run();
        } else {
            HLSManifestReader manifest_reader;
            manifest_reader.load_manifest(url);
            ios.run();
        }
    } catch(boost::program_options::error& e) {
        std::cout << e.what() << std::endl;
        std::cout << "Usage: " << argv[0] << "[options] <hls-url>" << std::endl;
        std::cout << desc << std::endl;
    } catch (std::exception& e)
    {
        FATAL "Exception: " << e.what();
    }
}
