import java.io.Serializable;

public class Address implements Serializable {
    int id;
    String address;

    public Address(){
        this.id=0;
        this.address=null;
    }

    public Address(int id, String address) { // Python devs: java constructors DON'T allow default values
        this.id=id;
        this.address=address;
    }
    public String toString() {
        return "Address:[" + this.id + "," + this.address + "]";
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }



}
