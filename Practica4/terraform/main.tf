provider "google" {
  project = var.proyecto
  region  = "europe-west1"
  zone    = "europe-west1-d"
}

resource "google_compute_instance" "terraform" {
  name         = "terraform"
  machine_type = "n1-standard-1"
  tags = ["web-server"]
  boot_disk {
    initialize_params {
      image = "projects/centos-cloud/global/images/centos-7-v20220822"
    }
  }
  network_interface {
    network = "default"
    access_config {
    }
  }
}

resource "google_compute_firewall" "default" {
  name    = "firewall"
  network = google_compute_network.default.name

  allow {
    protocol = "tcp"
    ports    = ["80", "8080", "22"]
  }

  source_tags = ["web-server"]
}

resource "google_compute_network" "default" {
  name = "test-network"
}
variable "proyecto"{
  type=string
  default = "valor-por-defecto-sobreescrito"
}

